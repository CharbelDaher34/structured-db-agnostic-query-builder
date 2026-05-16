"""
Generate system prompts for LLM query parsing.

Examples are derived from the user's actual schema (a representative enum
field, a numeric field, and a date field if any) so the LLM is grounded in
the real field names rather than a hard-coded domain.
"""

import json
from datetime import datetime
from typing import Any, Optional


class PromptGenerator:
    """
    Generates system prompts for LLM filter extraction.

    The system prompt has four parts:
      1. The data schema (source of truth for field names + enum values)
      2. The output JSON shape (compact reference)
      3. The hard rules
      4. A small number of worked examples — constructed from the user's
         own schema where possible, so the LLM sees field names it will
         actually need to emit.
    """

    # Enum fields with more values than this get truncated in the prompt to
    # keep context size sane. The full list still lives in model_info and the
    # discriminated-union validator enforces membership.
    MAX_ENUM_VALUES_INLINE = 50

    def __init__(
        self,
        model_info: dict[str, Any],
        max_enum_values_inline: int | None = None,
    ):
        self.model_info = model_info
        self.max_enum_values_inline = (
            max_enum_values_inline
            if max_enum_values_inline is not None
            else self.MAX_ENUM_VALUES_INLINE
        )

    # ------------------------------------------------------------------ schema

    def _summarise_model_info(self) -> dict[str, Any]:
        """Trim large enum value lists so the prompt stays a reasonable size."""
        if not self.max_enum_values_inline:
            return self.model_info

        summary: dict[str, Any] = {}
        for field, info in self.model_info.items():
            if (
                info.get("type") == "enum"
                and isinstance(info.get("values"), list)
                and len(info["values"]) > self.max_enum_values_inline
            ):
                trimmed = list(info["values"][: self.max_enum_values_inline])
                summary[field] = {
                    **info,
                    "values": trimmed,
                    "values_truncated": True,
                    "total_values": len(info["values"]),
                }
            else:
                summary[field] = info
        return summary

    # ------------------------------------------------- schema-grounded examples

    @staticmethod
    def _looks_like_id(field_name: str) -> bool:
        """Heuristic: skip 'id'-looking numeric fields for avg/sum examples."""
        leaf = field_name.split(".")[-1].lower()
        return leaf == "id" or leaf.endswith("_id") or leaf.endswith("id")

    def _pick_representative_fields(self) -> dict[str, Optional[Any]]:
        """
        Pick one field of each major type for use in example queries.

        - Enum: prefer the field with the smallest value-cardinality (so the
          example doesn't drown the model in unrelated values).
        - Numeric: prefer fields that don't look like IDs — averaging an ID
          column is semantically meaningless and confuses the model.
        """
        enum: Optional[tuple[str, list]] = None
        numeric: Optional[str] = None
        numeric_id_fallback: Optional[str] = None
        date: Optional[str] = None
        string: Optional[str] = None
        boolean: Optional[str] = None

        for name, info in self.model_info.items():
            t = info.get("type")
            if t == "enum" and info.get("values"):
                vals = info["values"]
                if enum is None or len(vals) < len(enum[1]):
                    enum = (name, vals)
            elif t == "number":
                if self._looks_like_id(name):
                    if numeric_id_fallback is None:
                        numeric_id_fallback = name
                elif numeric is None:
                    numeric = name
            elif t == "date" and date is None:
                date = name
            elif t == "string" and string is None:
                string = name
            elif t == "boolean" and boolean is None:
                boolean = name

        return {
            "enum": enum,
            "numeric": numeric or numeric_id_fallback,
            "date": date,
            "string": string,
            "boolean": boolean,
        }

    def _build_examples(self) -> list[str]:
        """
        Build a small set of worked examples using fields from the actual
        schema. Each example is a `**User:**` + ` ```json` fenced block.
        """
        picks = self._pick_representative_fields()
        examples: list[str] = []

        enum = picks["enum"]
        numeric = picks["numeric"]
        date = picks["date"]

        # --- Example 1: simple filter on an enum value
        if enum:
            field, values = enum
            value = values[0]
            examples.append(
                self._example(
                    user=f'Show me records where {field} is "{value}".',
                    slices=[
                        {
                            "conditions": [
                                {
                                    "type": "EnumFilter",
                                    "field": field,
                                    "operator": "is",
                                    "value": value,
                                }
                            ]
                        }
                    ],
                )
            )

        # --- Example 2: top-N + sort on a numeric
        if numeric:
            sort_field = numeric
            examples.append(
                self._example(
                    user=f"Show me the top 5 records sorted by {sort_field} descending.",
                    slices=[
                        {
                            "conditions": [],
                            "sort": [{"field": sort_field, "order": "desc"}],
                            "limit": 5,
                        }
                    ],
                )
            )

        # --- Example 3: group + aggregate (and an optional date interval)
        if enum and numeric:
            field, _ = enum
            examples.append(
                self._example(
                    user=f"For each {field}, what is the average {numeric}?",
                    slices=[
                        {
                            "conditions": [],
                            "group_by": [field],
                            "aggregations": [{"field": numeric, "type": "avg"}],
                        }
                    ],
                )
            )
        if date and numeric:
            examples.append(
                self._example(
                    user=f"Show me the monthly total of {numeric} grouped by {date}.",
                    slices=[
                        {
                            "conditions": [],
                            "group_by": [date],
                            "interval": "month",
                            "aggregations": [{"field": numeric, "type": "sum"}],
                        }
                    ],
                )
            )

        # --- Example 4: multi-slice comparison on two enum values
        if enum and len(enum[1]) >= 2:
            field, values = enum
            a, b = values[0], values[1]
            examples.append(
                self._example(
                    user=f'Compare records where {field} is "{a}" with those where it is "{b}".',
                    slices=[
                        {
                            "conditions": [
                                {"type": "EnumFilter", "field": field, "operator": "is", "value": a}
                            ]
                        },
                        {
                            "conditions": [
                                {"type": "EnumFilter", "field": field, "operator": "is", "value": b}
                            ]
                        },
                    ],
                )
            )

        # --- Example 5: an invalid enum value — show that we DROP the condition
        if enum:
            field, values = enum
            examples.append(self._invalid_enum_example(field, values))

        # If the schema is so sparse we picked nothing, fall back to a generic
        # abstract example to keep the section non-empty.
        if not examples:
            examples.append(
                self._example(
                    user="Show me everything.",
                    slices=[{"conditions": []}],
                    note=(
                        "(no schema fields available for a more specific example "
                        "— rely on the schema in section 2)"
                    ),
                )
            )

        return examples

    @staticmethod
    def _example(
        user: str,
        slices: list[dict[str, Any]],
        note: Optional[str] = None,
    ) -> str:
        body = json.dumps({"filters": slices}, indent=2)
        note_line = f"\n*Note*: {note}" if note else ""
        return f'**User**: "{user}"\n```json\n{body}\n```{note_line}'

    def _invalid_enum_example(self, field: str, valid_values: list[Any]) -> str:
        # Pick a value that is NOT in the enum list
        invalid_value = "DEFINITELY_NOT_A_VALID_VALUE"
        if isinstance(valid_values[0], str) and invalid_value in valid_values:
            invalid_value = invalid_value + "_X"
        return (
            f'**User**: "Show me records where {field} is '
            f'\\"{invalid_value}\\"."\n'
            f"```json\n"
            f"{json.dumps({'filters': [{'conditions': []}]}, indent=2)}\n"
            f"```\n"
            f"*Note*: `{invalid_value}` is not in the valid values for `{field}` "
            f"(`{valid_values[:5]}{'...' if len(valid_values) > 5 else ''}`), "
            f"so we DROP the condition instead of inventing a value. The query "
            f"returns all records."
        )

    # --------------------------------------------------------- main entry point

    def generate_system_prompt(self) -> str:
        """Render the full system prompt as a single string."""
        today = datetime.now().strftime("%Y-%m-%d")
        schema_json = json.dumps(self._summarise_model_info(), indent=2)
        examples = "\n\n".join(self._build_examples())

        return _PROMPT_TEMPLATE.format(
            today=today,
            schema_json=schema_json,
            examples=examples,
        )


_PROMPT_TEMPLATE = """\
Today is {today}.

## 1. Your task
Convert the user's natural-language question into a single JSON object that strictly conforms to the `QueryFilters` schema. Output only the JSON — no commentary, no markdown fences.

## 2. Data schema (the ONLY valid field names)
Every `field` value you emit MUST be a key from the JSON below. If a user's term has no matching field, DROP that condition rather than invent a field.

Enum fields list their valid values inline. If `"values_truncated": true`, only a sample of the full set is shown — the full count is in `total_values`, and the validator enforces membership against the full set.

```json
{schema_json}
```

## 3. Output shape

The top-level object is `{{ "filters": [...] }}`. Each entry in `filters` is a slice that describes one set of records. A slice has these keys:

| Key | Type | When to use |
|---|---|---|
| `conditions` | list of typed filters | AND-joined filters on individual fields |
| `sort` | list of `{{field, order}}` | When the user mentions ordering ("highest", "oldest"...) |
| `limit` | int | ONLY when the user names a specific number ("top 5", "first 10") |
| `group_by` | list of field names | When the user asks "per X", "by X", "for each X" |
| `aggregations` | list of `{{field, type, having_operator?, having_value?}}` | Requires `group_by`; types: sum / avg / count / min / max |
| `interval` | day / week / month / year | ONLY with a date `group_by`; defaults to month |

Each filter in `conditions` is one of:

| Filter | For schema type | Operators |
|---|---|---|
| `StringFilter` | `string` | is, different, contains, isin, notin, exists |
| `NumberFilter` | `number` | <, >, is, different, between, isin, notin, exists |
| `DateFilter` | `date` | <, >, is, different, between, exists |
| `BooleanFilter` | `boolean` | is, different, exists |
| `EnumFilter` | `enum` | is, different, isin, notin, exists |

Filter shape: `{{ "type": "<Filter>", "field": "<schema_field>", "operator": "<op>", "value": <value or list> }}`. For `between`/range queries on numbers/dates, use a 2-element list.

## 4. Hard rules

1. **Schema fidelity.** Field names come from section 2. Never use the field names from the examples in section 5 unless they happen to appear in the schema.
2. **Filter type matches schema type.** A `string` field can only carry a `StringFilter`, an `enum` field can only carry an `EnumFilter`, etc. Picking the wrong filter type will fail validation.
3. **Invalid enum values are dropped, not invented.** If the user names a value that isn't in the enum list, omit the condition entirely.
4. **`aggregations` and `interval` require `group_by`.** Don't emit them in an ungrouped slice.
5. **`interval` requires a date field in `group_by`.** Don't use it when grouping by a non-date field.
6. **Comparisons → multiple slices.** "Compare A and B" produces two slices, one per side.
7. **Default = no limit.** Add `limit` only when the user names a specific number. "Recent" → sort desc, no limit.
8. **Resolve relative dates to absolute ranges.** "Last month" → `between [first, last]` using today's date.

## 5. Worked examples (using THIS schema's field names)

{examples}

━━━━━━━━━━━━━━━━━━━━━━━
After reading the user's question, output ONLY the JSON object (starting with `{{ "filters": [...] }}`). No explanation, no markdown fences.
━━━━━━━━━━━━━━━━━━━━━━━
"""
