"""
Course Prerequisite to JSON Adjacency Graph Converter with Pydantic
Uses Pydantic models for type safety and intuitive data structures
"""

import pandas as pd
import json
from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field, validator
from enum import Enum

from parser import (
    PrerequisiteParser,
    CourseWithMetadata,
    CommaExpression,
    OrExpression,
    AndExpression,
    GroupedExpression
)


class GradeEnum(str, Enum):
    """Valid grade values"""
    A_PLUS = "A+"
    A = "A"
    A_MINUS = "A-"
    B_PLUS = "B+"
    B = "B"
    B_MINUS = "B-"
    C_PLUS = "C+"
    C = "C"
    C_MINUS = "C-"
    D_PLUS = "D+"
    D = "D"
    D_MINUS = "D-"
    F = "F"
    CR = "CR"  # Credit
    NC = "NC"  # No Credit


class Course(BaseModel):
    """Represents a single course in the prerequisite structure"""
    coursename: str = Field(...,
                            description="Course name in format 'SUBJECT NUMBER'")
    id: str = Field(..., description="Unique course identifier")
    minimum_grade: GradeEnum = Field(
        default=GradeEnum.D, description="Minimum required grade")

    @validator('coursename')
    def validate_coursename_format(cls, v):
        """Ensure coursename follows 'SUBJECT NUMBER' format"""
        if not v or ' ' not in v:
            raise ValueError("Course name must be in format 'SUBJECT NUMBER'")
        return v.strip()

    def __str__(self) -> str:
        return f"{self.coursename} (Min Grade: {self.minimum_grade})"


class AndGroup(BaseModel):
    """Represents an AND group in the prerequisite structure"""
    courses: List[Course] = Field(...,
                                  description="List of courses in OR relationship")
    canBeTakenConcurrently: bool = Field(
        default=False, description="Whether this prerequisite can be taken concurrently")

    @validator('courses')
    def courses_not_empty(cls, v):
        """Ensure courses list is not empty"""
        if not v:
            raise ValueError("AND group must contain at least one course")
        return v

    @property
    def is_single_requirement(self) -> bool:
        """True if this group has only one course (required course)"""
        return len(self.courses) == 1

    @property
    def is_choice_requirement(self) -> bool:
        """True if this group has multiple courses (choose one)"""
        return len(self.courses) > 1

    def __str__(self) -> str:
        if self.is_single_requirement:
            course_str = f"Requires: {self.courses[0]}"
        else:
            course_names = [course.coursename for course in self.courses]
            course_str = f"Choose one of: {', '.join(course_names)}"

        if self.canBeTakenConcurrently:
            course_str += " (Can be taken concurrently)"

        return course_str


class CoursePrerequisites(BaseModel):
    """Represents all prerequisites for a single course"""
    course_id: str = Field(..., description="Unique identifier of the course")
    course_name: str = Field(..., description="Human-readable course name")
    and_groups: List[AndGroup] = Field(
        ..., description="List of AND groups (all must be satisfied)")

    @validator('and_groups')
    def and_groups_not_empty(cls, v):
        """Ensure and_groups list is not empty"""
        if not v:
            raise ValueError(
                "Course prerequisites must contain at least one AND group")
        return v

    @property
    def total_prerequisite_courses(self) -> int:
        """Count total number of prerequisite courses across all groups"""
        return sum(len(group.courses) for group in self.and_groups)

    @property
    def has_choices(self) -> bool:
        """True if any AND group has multiple course options"""
        return any(group.is_choice_requirement for group in self.and_groups)

    def __str__(self) -> str:
        result = f"Prerequisites for {self.course_name}:\n"
        for i, group in enumerate(self.and_groups, 1):
            result += f"  AND Group {i}: {group}\n"
        return result.strip()


class AdjacencyGraph(BaseModel):
    """Complete adjacency graph containing all course prerequisites"""
    prerequisites: Dict[str, List[AndGroup]] = Field(
        default_factory=dict,
        description="Mapping from course ID to list of AND groups"
    )

    def add_course_prerequisites(self, course_prereqs: CoursePrerequisites):
        """Add prerequisites for a course to the graph"""
        self.prerequisites[course_prereqs.course_id] = course_prereqs.and_groups

    def get_course_prerequisites(self, course_id: str) -> Optional[CoursePrerequisites]:
        """Get prerequisites for a specific course"""
        if course_id not in self.prerequisites:
            return None

        # We don't store course_name in the graph, so use course_id as name
        return CoursePrerequisites(
            course_id=course_id,
            course_name=course_id,
            and_groups=self.prerequisites[course_id]
        )

    @property
    def course_count(self) -> int:
        """Number of courses with prerequisites"""
        return len(self.prerequisites)

    @property
    def total_prerequisite_relationships(self) -> int:
        """Total number of prerequisite relationships across all courses"""
        return sum(
            sum(len(group.courses) for group in and_groups)
            for and_groups in self.prerequisites.values()
        )

    def to_dict(self) -> Dict[str, List[Dict]]:
        """Convert to dictionary format for JSON serialization"""
        return {
            course_id: [group.dict() for group in and_groups]
            for course_id, and_groups in self.prerequisites.items()
        }

    def __str__(self) -> str:
        return f"Adjacency Graph with {self.course_count} courses and {self.total_prerequisite_relationships} prerequisite relationships"


class CourseData(BaseModel):
    """Represents a course record from CSV"""
    id: str
    subject_id: str
    course_number: str
    title: str
    prerequisites: Optional[str] = None

    @property
    def full_course_name(self) -> str:
        """Get full course name in format 'SUBJECT NUMBER'"""
        return f"{self.subject_id} {self.course_number}"

    @property
    def has_prerequisites(self) -> bool:
        """Check if course has prerequisites"""
        return bool(self.prerequisites and self.prerequisites.strip())


class ParseResult(BaseModel):
    """Result of parsing a course's prerequisites"""
    course_data: CourseData
    success: bool
    prerequisites: Optional[CoursePrerequisites] = None
    error_message: Optional[str] = None

    def __str__(self) -> str:
        if self.success:
            return f"✅ {self.course_data.full_course_name}: Successfully parsed"
        else:
            return f"❌ {self.course_data.full_course_name}: {self.error_message}"


class ParsingStats(BaseModel):
    """Statistics from parsing all courses"""
    total_courses: int = 0
    successful_parses: int = 0
    failed_parses: int = 0

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage"""
        if self.total_courses == 0:
            return 0.0
        return (self.successful_parses / self.total_courses) * 100

    def __str__(self) -> str:
        return f"Parsing Stats: {self.successful_parses}/{self.total_courses} successful ({self.success_rate:.1f}%)"


class AdjacencyGraphGenerator:
    """Converts parsed prerequisites to JSON adjacency graph format using Pydantic models"""

    def __init__(self):
        self.parser = PrerequisiteParser()

    def load_courses_csv(self, filename: str = 'courses.csv') -> List[CourseData]:
        """Load courses from CSV file and return as Pydantic models"""
        try:
            df = pd.read_csv(filename)
            print(f"✅ Loaded {len(df)} courses from {filename}")

            courses = []
            for _, row in df.iterrows():
                try:
                    course = CourseData(
                        id=row['id'],
                        subject_id=row['subject_id'],
                        course_number=row['course_number'],
                        title=row['title'],
                        prerequisites=row.get('prerequisites')
                    )
                    courses.append(course)
                except Exception as e:
                    print(f"⚠️ Skipping invalid course row: {e}")

            return courses

        except FileNotFoundError:
            print(f"❌ Error: {filename} not found")
            return []
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return []

    def convert_ast_to_and_groups(self, ast) -> List[AndGroup]:
        """Convert AST to list of AndGroup models"""
        and_groups = []
        self._process_node_for_adjacency(ast, and_groups)
        return and_groups

    def _process_node_for_adjacency(self, node, and_groups: List[AndGroup]):
        """Process AST node and build AND groups with OR courses"""

        if isinstance(node, CourseWithMetadata):
            # Single course - create a new AND group with this course
            course = Course(
                coursename=f"{node.course.subject} {node.course.number}",
                id=f"{node.course.subject} {node.course.number}",
                minimum_grade=GradeEnum(
                    node.grade.grade) if node.grade else GradeEnum.D
            )

            # Set canBeTakenConcurrently based on whether help_text exists and is non-empty
            can_be_concurrent = bool(
                node.help_text and node.help_text.content.strip())

            and_group = AndGroup(
                courses=[course],
                canBeTakenConcurrently=can_be_concurrent
            )
            and_groups.append(and_group)

        elif isinstance(node, CommaExpression):
            # Comma typically means OR in prerequisites
            self._process_or_group(node.operands, and_groups)

        elif isinstance(node, OrExpression):
            # OR expression - all operands go in same AND group
            self._process_or_group(node.operands, and_groups)

        elif isinstance(node, AndExpression):
            # AND expression - each operand becomes separate AND group
            for operand in node.operands:
                self._process_node_for_adjacency(operand, and_groups)

        elif isinstance(node, GroupedExpression):
            # Process the grouped expression
            self._process_node_for_adjacency(node.expression, and_groups)

    def _process_or_group(self, operands: List, and_groups: List[AndGroup]):
        """Process a list of operands that should be ORed together"""
        or_courses = []
        has_concurrent_indicator = False

        for operand in operands:
            if isinstance(operand, CourseWithMetadata):
                course = Course(
                    coursename=f"{operand.course.subject} {
                        operand.course.number}",
                    id=f"{operand.course.subject} {operand.course.number}",
                    minimum_grade=GradeEnum(
                        operand.grade.grade) if operand.grade else GradeEnum.D
                )
                or_courses.append(course)

                # Check if any operand has helper text indicating concurrent enrollment
                if operand.help_text and operand.help_text.content.strip():
                    has_concurrent_indicator = True
            else:
                # For complex nested structures, recursively process
                temp_groups = []
                self._process_node_for_adjacency(operand, temp_groups)
                # Flatten into or_courses if possible
                for group in temp_groups:
                    or_courses.extend(group.courses)
                    if group.canBeTakenConcurrently:
                        has_concurrent_indicator = True

        if or_courses:
            and_group = AndGroup(
                courses=or_courses,
                canBeTakenConcurrently=has_concurrent_indicator
            )
            and_groups.append(and_group)

    def parse_single_course(self, course_data: CourseData) -> ParseResult:
        """Parse prerequisites for a single course"""
        if not course_data.has_prerequisites:
            return ParseResult(
                course_data=course_data,
                success=False,
                error_message="No prerequisites found"
            )

        try:
            # Parse using the existing parser
            ast = self.parser.parse(course_data.prerequisites)

            # Convert to AndGroup models
            and_groups = self.convert_ast_to_and_groups(ast)

            if and_groups:
                prerequisites = CoursePrerequisites(
                    course_id=course_data.id,
                    course_name=course_data.full_course_name,
                    and_groups=and_groups
                )

                return ParseResult(
                    course_data=course_data,
                    success=True,
                    prerequisites=prerequisites
                )
            else:
                return ParseResult(
                    course_data=course_data,
                    success=False,
                    error_message="No valid prerequisites parsed"
                )

        except Exception as e:
            return ParseResult(
                course_data=course_data,
                success=False,
                error_message=str(e)
            )

    def generate_adjacency_graph(self, csv_filename: str = 'courses.csv') -> AdjacencyGraph:
        """Generate complete adjacency graph from CSV"""

        # Load CSV data
        courses = self.load_courses_csv(csv_filename)
        if not courses:
            return AdjacencyGraph()

        # Filter courses with prerequisites
        courses_with_prereqs = [c for c in courses if c.has_prerequisites]
        print(f"🔍 Found {len(courses_with_prereqs)
                         } courses with prerequisites")

        adjacency_graph = AdjacencyGraph()
        stats = ParsingStats(total_courses=len(courses_with_prereqs))

        parse_results = []

        for course_data in courses_with_prereqs:
            print(f"\n📚 Processing: {course_data.full_course_name}")
            print(f"   Prerequisites: {course_data.prerequisites}")

            result = self.parse_single_course(course_data)
            parse_results.append(result)

            if result.success:
                adjacency_graph.add_course_prerequisites(result.prerequisites)
                stats.successful_parses += 1

                # print( f"   ✅ Successfully parsed - {len(result.prerequisites.and_groups)} AND group(s)")

                # Show the groups
                # for i, group in enumerate(result.prerequisites.and_groups):
                #     print(f"      Group {i+1}: {group}")

            else:
                stats.failed_parses += 1
                print(f"   ❌ Parse failed: {result.error_message}")

        # Print summary
        print(f"\n{'='*60}")
        print("📊 SUMMARY")
        print(f"{'='*60}")
        print(stats)
        # print(adjacency_graph)

        return adjacency_graph

    def save_to_json(self, adjacency_graph: AdjacencyGraph, filename: str = 'course_adjacency_graph.json'):
        """Save adjacency graph to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(adjacency_graph.to_dict(), f,
                          indent=2, ensure_ascii=False)
            print(f"💾 Adjacency graph saved to {filename}")
        except Exception as e:
            print(f"❌ Error saving to JSON: {e}")

    def print_sample_output(self, adjacency_graph: AdjacencyGraph, max_samples: int = 3):
        """Print sample output for verification"""
        print(f"\n{'='*60}")
        print("📋 SAMPLE OUTPUT")
        print(f"{'='*60}")

        count = 0
        for course_id, and_groups in adjacency_graph.prerequisites.items():
            if count >= max_samples:
                break

            print(f"\nCourse ID: {course_id}")
            course_prereqs = CoursePrerequisites(
                course_id=course_id,
                course_name=course_id,
                and_groups=and_groups
            )
            print(course_prereqs)
            count += 1

        if adjacency_graph.course_count > max_samples:
            print(f"\n... and {
                  adjacency_graph.course_count - max_samples} more courses")
