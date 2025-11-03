"""
Database-agnostic Pydantic model builder.

Builds Pydantic models from normalized schema information.
"""

import inspect
from typing import Any, Dict, List, Optional, Union, get_args, get_origin
from datetime import datetime, date
from enum import Enum

from pydantic import BaseModel, Field, create_model

from query_builder.schema.type_mappings import TypeMapper


class ModelBuilder:
    """
    Builds Pydantic models from normalized schema.
    
    This class is database-agnostic and only works with normalized schema data.
    """
    
    IGNORED_FIELD_TYPES = {"alias", "unknown"}
    
    def __init__(
        self,
        schema: Dict[str, Any],
        fields_to_ignore: Optional[List[str]] = None,
        enum_fields: Optional[Dict[str, List[Any]]] = None,
    ):
        """
        Initialize model builder.
        
        Args:
            schema: Normalized schema dictionary (field_path -> field_info)
            fields_to_ignore: List of field names to skip
            enum_fields: Dictionary of field paths to enum values
        """
        self.schema = schema
        self.fields_to_ignore = fields_to_ignore or []
        self.enum_fields = enum_fields or {}
        self._model_class: Optional[type[BaseModel]] = None
        self._model_info: Optional[Dict[str, Any]] = None
    
    def build(self, model_name: str = "GeneratedModel") -> type[BaseModel]:
        """
        Build Pydantic model from schema.
        
        Args:
            model_name: Name for the generated model class
            
        Returns:
            Generated Pydantic model class
        """
        if self._model_class is None:
            self._model_class = self._build_pydantic_model(
                self.schema, model_name
            )
        return self._model_class
    
    def get_model_info(self) -> Dict[str, Any]:
        """
        Extract flattened field information from the model.
        
        Returns:
            Dictionary mapping field paths to field metadata
        """
        if self._model_info is None:
            model = self.build()
            self._model_info = self._extract_model_info(model)
        return self._model_info
    
    def _build_pydantic_model(
        self,
        schema: Dict[str, Any],
        model_name: str,
        current_path: str = "",
    ) -> type[BaseModel]:
        """
        Recursively build Pydantic model from schema.
        
        Args:
            schema: Schema dictionary or nested properties
            model_name: Name for this model/submodel
            current_path: Current field path (for nested objects)
            
        Returns:
            Pydantic model class
        """
        fields: Dict[str, tuple] = {}
        
        # Group fields by their parent (for nested objects)
        grouped_fields: Dict[str, Dict[str, Any]] = {}
        direct_fields: Dict[str, Any] = {}
        
        for field_path, field_info in schema.items():
            # Skip fields starting with underscore (Pydantic doesn't allow them)
            field_name_only = field_path.split(".")[-1]
            if field_name_only.startswith("_"):
                continue
            
            # Skip if this is a sub-field of current path (will be handled by recursion)
            if current_path and not field_path.startswith(current_path + "."):
                continue
            
            # Get relative field name
            if current_path:
                relative_path = field_path[len(current_path) + 1:]
            else:
                relative_path = field_path
            
            # Skip if this is a nested field (contains dots)
            if "." in relative_path:
                parent_field = relative_path.split(".")[0]
                if parent_field not in grouped_fields:
                    grouped_fields[parent_field] = {}
                # Store full path
                grouped_fields[parent_field][field_path] = field_info
            else:
                direct_fields[relative_path] = field_info
        
        # Process direct fields
        for field_name, field_info in direct_fields.items():
            if field_name in self.fields_to_ignore:
                continue
            
            # Skip fields starting with underscore (Pydantic doesn't allow them)
            if field_name.startswith("_"):
                continue
            
            field_type = field_info.get("type")
            
            if field_type in self.IGNORED_FIELD_TYPES:
                continue
            
            full_field_path = f"{current_path}.{field_name}" if current_path else field_name
            
            # Check if this field has nested properties
            if field_name in grouped_fields:
                # Build nested model
                nested_model_name = f"{model_name}_{field_name.capitalize()}"
                nested_model = self._build_pydantic_model(
                    grouped_fields[field_name],
                    nested_model_name,
                    full_field_path,
                )
                
                if field_type == "array":
                    py_type = List[nested_model]
                else:
                    py_type = nested_model
            else:
                # Check for enum values
                enum_values = self.enum_fields.get(full_field_path) or field_info.get("values")
                
                if enum_values:
                    py_type = self._create_enum_type(field_name, model_name, enum_values)
                elif field_type == "array":
                    item_type_str = field_info.get("item_type", "string")
                    item_py_type = TypeMapper.get_python_type(item_type_str)
                    py_type = List[item_py_type]
                else:
                    py_type = TypeMapper.get_python_type(field_type)
            
            fields[field_name] = self._get_field_definition(py_type, field_info)
        
        return create_model(model_name, **fields)  # type: ignore[arg-type]
    
    def _create_enum_type(
        self, field_name: str, model_name: str, values: List[Any]
    ) -> type[Enum]:
        """Create an Enum type from values."""
        enum_class_name = f"{model_name}_{field_name.capitalize()}Enum"
        enum_members = {}
        
        for i, value in enumerate(values):
            member_name = str(value)
            if isinstance(value, str):
                # Sanitize enum member name
                member_name = (
                    value.replace(" ", "_")
                    .replace("-", "_")
                    .replace("'", "")
                    .replace(".", "_")
                )
                if not member_name or (not member_name[0].isalpha() and member_name[0] != "_"):
                    member_name = f"_{member_name}"
                member_name = "".join(c for c in member_name if c.isalnum() or c == "_") or f"VALUE_{i}"
            else:
                member_name = f"VALUE_{i}"
            
            enum_members[member_name.upper()] = value
        
        return Enum(enum_class_name, enum_members)
    
    def _get_field_definition(self, py_type: Any, field_info: Dict[str, Any]) -> tuple:
        """Get Pydantic field definition (type, Field(...))."""
        is_required = field_info.get("required", False)
        
        if isinstance(py_type, type) and issubclass(py_type, Enum):
            return (Optional[py_type], Field(default=None))
        
        if isinstance(py_type, type) and issubclass(py_type, BaseModel):
            if is_required:
                return (py_type, Field(...))
            return (Optional[py_type], Field(default=None))
        
        if hasattr(py_type, "__origin__") and py_type.__origin__ is list:
            args = getattr(py_type, "__args__", ())
            if args and isinstance(args[0], type) and issubclass(args[0], BaseModel):
                if is_required:
                    return (py_type, Field(...))
                return (Optional[py_type], Field(default=None))
            return (Optional[py_type], Field(default=None))
        
        return (Optional[py_type], Field(default=None))
    
    def _extract_model_info(
        self, model_class: type[BaseModel], prefix: str = ""
    ) -> Dict[str, Any]:
        """Extract flattened field information from model."""
        info = {}
        
        for field_name, field_info in model_class.model_fields.items():
            field_type = field_info.annotation
            origin, args = get_origin(field_type), get_args(field_type)
            full_field_name = f"{prefix}.{field_name}" if prefix else field_name
            
            # Handle Optional types
            if origin is Union:
                non_none_types = [arg for arg in args if arg is not type(None)]
                if non_none_types:
                    field_type = non_none_types[0]
                    origin, args = get_origin(field_type), get_args(field_type)
            
            # Handle different field types
            if inspect.isclass(field_type) and issubclass(field_type, Enum):
                info[full_field_name] = {
                    "type": "enum",
                    "values": [e.value for e in field_type],
                }
            elif inspect.isclass(field_type) and issubclass(field_type, BaseModel):
                info.update(self._extract_model_info(field_type, full_field_name))
            elif origin is list or origin is List:
                list_info = self._get_list_field_info(args, full_field_name)
                if isinstance(list_info, dict) and any(
                    "is_array_item" in v for v in list_info.values()
                ):
                    info.update(list_info)
                else:
                    info[full_field_name] = list_info
            elif field_type is str:
                info[full_field_name] = {"type": "string"}
            elif field_type in (int, float):
                info[full_field_name] = {"type": "number"}
            elif field_type is bool:
                info[full_field_name] = {"type": "boolean"}
            elif field_type in (date, datetime):
                info[full_field_name] = {"type": "date"}
            else:
                info[full_field_name] = {"type": self._get_simple_type_name(field_type)}
        
        return info
    
    def _get_list_field_info(self, args, full_field_name: str) -> Dict[str, Any]:
        """Get information about list/array fields."""
        if not args:
            return {"type": "array", "item_type": "unknown"}
        
        list_item_type = args[0]
        
        if inspect.isclass(list_item_type) and issubclass(list_item_type, BaseModel):
            nested_info = self._extract_model_info(list_item_type, full_field_name)
            for nested_field_info in nested_info.values():
                nested_field_info["is_array_item"] = True
            return nested_info
        
        if inspect.isclass(list_item_type) and issubclass(list_item_type, Enum):
            return {
                "type": "array",
                "item_type": "enum",
                "values": [e.value for e in list_item_type],
            }
        
        return {
            "type": "array",
            "item_type": self._get_simple_type_name(list_item_type),
        }
    
    def _get_simple_type_name(self, field_type) -> str:
        """Get simple type name as string."""
        if hasattr(field_type, "__name__"):
            return field_type.__name__
        if hasattr(field_type, "_name"):
            return field_type._name
        return str(field_type)

