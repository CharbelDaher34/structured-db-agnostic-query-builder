from typing import List, Optional
from pydantic import BaseModel, Field, HttpUrl, EmailStr
from datetime import date
from enum import Enum


# Enum reused from Candidate model
class EducationLevel(str, Enum):
    high_school = "High School"
    associate = "Associate"
    bachelor = "Bachelor’s"
    master = "Master’s"
    doctorate = "Doctorate"
    diploma = "Diploma"
    certificate = "Certificate"
    other = "Other"


# Skill model with required flag
class JobSkillItem(BaseModel):
    name: str = Field(..., description="Name of the skill (e.g., Python).")
    category: str = Field(
        ..., description="Category of the skill (e.g., Programming Language)."
    )
    level: str = Field(
        ..., description="Skill proficiency (e.g., Beginner, Intermediate, Expert)."
    )
    required: bool = Field(
        ..., description="Indicates if the skill is required or optional."
    )


# Location model (same as in candidate, flat fields)
class JobLocation(BaseModel):
    city_name: Optional[str] = Field(None, description="City where the job is located.")
    county_name: Optional[str] = Field(None, description="County of the job location.")
    state_name: Optional[str] = Field(None, description="State of the job location.")
    geo_coordinates: Optional[str] = Field(
        None, description="Latitude and longitude as string."
    )
    postal_code: Optional[str] = Field(None, description="Postal code.")
    street_address: Optional[str] = Field(None, description="Street address.")


# Main Job model
class Job(BaseModel):
    job_id: str = Field(..., description="Unique identifier for the job.")
    job_title: str = Field(
        ..., description="Title of the job (e.g., Software Engineer)."
    )
    occupation_code: Optional[str] = Field(
        None, description="Standardized occupation code (e.g., ONET/SOC)."
    )

    # Employer details flattened
    employer_name: str = Field(..., description="Name of the hiring company.")
    naics_code: Optional[str] = Field(
        None, description="NAICS industry classification code."
    )
    website: Optional[HttpUrl] = Field(None, description="Company's official website.")

    # Education Requirements flattened
    education_level: Optional[EducationLevel] = Field(
        None, description="Minimum required education level."
    )
    degree_type: Optional[str] = Field(
        None, description="Required degree type (e.g., BS, MSc)."
    )
    subject: Optional[str] = Field(
        None, description="Required major or field of study."
    )
    education_required: Optional[bool] = Field(
        None, description="Whether the education requirement is mandatory."
    )

    # Skills and experience
    skills: List[JobSkillItem] = Field(
        ..., description="List of required or optional skills."
    )
    experience_min: Optional[float] = Field(
        None, description="Minimum years of experience required."
    )
    experience_max: Optional[float] = Field(
        None, description="Maximum years of experience accepted."
    )

    # Salary and logistics
    salary_min: Optional[float] = Field(None, description="Minimum offered salary.")
    salary_max: Optional[float] = Field(None, description="Maximum offered salary.")
    salary_period: Optional[str] = Field(
        None, description="Period of salary (e.g., yearly, monthly, hourly)."
    )
    currency: Optional[str] = Field(
        None, description="Currency of the salary (e.g., USD, EUR)."
    )

    remote: Optional[bool] = Field(
        None, description="Whether the job allows remote work."
    )
    locations: List[JobLocation] = Field(..., description="List of job locations.")

    date_posted: Optional[date] = Field(
        None, description="Date when the job was posted."
    )
    date_expired: Optional[date] = Field(
        None, description="Date when the job posting expires."
    )

    is_internship: Optional[bool] = Field(
        False, description="Whether the job is an internship."
    )
    is_staffing: Optional[bool] = Field(
        False, description="Whether this job is posted by a staffing agency."
    )
    full_time: Optional[bool] = Field(
        False, description="Whether the job is full-time."
    )

    # Contact Info flattened
    prefix: Optional[str] = Field(
        None, description="Contact person's title (e.g., Mr., Dr.)."
    )
    full_name: Optional[str] = Field(
        None, description="Full name of the contact person."
    )
    first_name: Optional[str] = Field(None, description="First name of the contact.")
    middle_name: Optional[str] = Field(None, description="Middle name of the contact.")
    last_name: Optional[str] = Field(None, description="Last name of the contact.")
    suffix: Optional[str] = Field(
        None, description="Suffix for the contact (e.g., Jr., Sr.)."
    )

    telephones: List[str] = Field(
        default_factory=list, description="List of contact phone numbers."
    )
    email_addresses: List[EmailStr] = Field(
        default_factory=list, description="List of contact email addresses."
    )
    web_addresses: List[HttpUrl] = Field(
        default_factory=list,
        description="List of URLs for more information or applying.",
    )
