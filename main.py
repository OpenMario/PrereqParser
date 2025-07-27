"""
Course Prerequisite Parser using Lark
Install: pip install lark pandas
"""

from lark import Lark, Transformer, Tree
from typing import List, Dict, Any, Optional
import pandas as pd
import re
import sys

# EBNF Grammar for Lark (fixed tokenization with raw strings)
# PREREQUISITE_GRAMMAR = r"""
#     start: or_expression
#
#     ?or_expression: and_expression
#                   | or_expression "or"i and_expression
#
#     ?and_expression: course_term
#                    | and_expression "and"i course_term
#
#     ?course_term: course_with_metadata
#                 | "(" or_expression ")"  -> grouped_expression
#
#     course_with_metadata: course_code grade_requirement? help_text?
#
#     course_code: SUBJECT_ID COURSE_NUMBER
#
#     grade_requirement: "[" MIN_GRADE_TEXT GRADE_VALUE "]"
#
#     help_text: "(" help_content ")"
#
#     help_content: HELP_TEXT
#
#     SUBJECT_ID: /[A-Z]{2,5}/
#     COURSE_NUMBER: /[A-Z0-9]+/
#     GRADE_VALUE: /[A-F][+-]?/
#     MIN_GRADE_TEXT: /[Mm]in\s+[Gg]rade\s*:\s*/
#     HELP_TEXT: /[^)]+/
#
#     %import common.WS
#     %ignore WS
# """

PREREQUISITE_GRAMMAR = r"""
    start: or_expression

    ?or_expression: and_expression
                  | or_expression "or"i and_expression

    ?and_expression: course_term
                   | and_expression "and"i course_term

    ?course_term: course_with_metadata
                | "(" or_expression ")"  -> grouped_expression

    course_with_metadata: course_code grade_requirement? help_text?

    course_code: SUBJECT_ID COURSE_NUMBER

    grade_requirement: "[" MIN_GRADE_TEXT GRADE_VALUE "]"

    help_text: OPEN_PAREN HELP_TEXT_CONTENT CLOSE_PAREN

    OPEN_PAREN: "("
    CLOSE_PAREN: ")"

    // Change from '*' to '+'
    HELP_TEXT_CONTENT: /[^)]+/ // Matches one or more characters that are not ')'

    SUBJECT_ID: /[A-Z]{2,5}/
    COURSE_NUMBER: /[A-Z0-9]+/
    GRADE_VALUE: /[A-F][+-]?|CR|NC/
    MIN_GRADE_TEXT: /[Mm]in\s+[Gg]rade\s*:\s*/

    %import common.WS
    %ignore WS
"""


# AST Node Classes


class CourseCode:
    def __init__(self, subject: str, number: str):
        self.subject = subject
        self.number = number

    def __str__(self):
        return f"{self.subject} {self.number}"

    def __repr__(self):
        return f"CourseCode({self.subject}, {self.number})"


class GradeRequirement:
    def __init__(self, grade: str):
        self.grade = grade

    def __repr__(self):
        return f"GradeRequirement({self.grade})"


class HelpText:
    def __init__(self, content: str):
        self.content = content.strip()

    def __repr__(self):
        return f"HelpText({self.content})"


class CourseWithMetadata:
    def __init__(self, course: CourseCode, grade: Optional[GradeRequirement] = None,
                 help_text: Optional[HelpText] = None):
        self.course = course
        self.grade = grade
        self.help_text = help_text

    def __repr__(self):
        return f"CourseWithMetadata({self.course}, {self.grade}, {self.help_text})"


class AndExpression:
    def __init__(self, operands: List[Any]):
        self.operands = operands

    def __repr__(self):
        return f"AndExpression({self.operands})"


class OrExpression:
    def __init__(self, operands: List[AndExpression]):
        self.operands = operands

    def __repr__(self):
        return f"OrExpression({self.operands})"


class GroupedExpression:
    def __init__(self, expression: OrExpression):
        self.expression = expression

    def __repr__(self):
        return f"GroupedExpression({self.expression})"

# Transform parse tree to AST


class PrerequisiteTransformer(Transformer):
    def course_code(self, args):
        subject, number = args
        return CourseCode(str(subject), str(number))

    def grade_requirement(self, args):
        # args = [MIN_GRADE_TEXT, GRADE_VALUE] - brackets are handled by grammar
        return GradeRequirement(str(args[1]))  # Grade value is at index 1

    def help_text(self, args):
        # args = ["(", help_content, ")"]
        return HelpText(str(args[1]))

    # def help_content(self, args):
    #     return args[0]  # Just pass through the HELP_TEXT token

    def course_with_metadata(self, args):
        course = args[0]
        grade = None
        help_text = None

        # Process remaining args
        for arg in args[1:]:
            if isinstance(arg, GradeRequirement):
                grade = arg
            elif isinstance(arg, HelpText):
                help_text = arg

        return CourseWithMetadata(course, grade, help_text)

    def grouped_expression(self, args):
        # args = [or_expression]
        return GroupedExpression(args[0])

    def and_expression(self, args):
        if len(args) == 1:
            return args[0]  # Single term, no need to wrap
        return AndExpression(args)

    def or_expression(self, args):
        if len(args) == 1:
            return args[0]  # Single term, no need to wrap
        return OrExpression(args)

    def start(self, args):
        return args[0]  # Return the top-level expression

# Main Parser Class


class PrerequisiteParser:
    def __init__(self):
        # ADD THIS DEBUG PRINT STATEMENT
        print("\n--- Current PREREQUISITE_GRAMMAR being used: ---")
        print(PREREQUISITE_GRAMMAR)
        print("--------------------------------------------------\n")
        self.parser = Lark(PREREQUISITE_GRAMMAR, parser='lalr')
        self.transformer = PrerequisiteTransformer()

    def parse(self, text: str):
        """Parse prerequisite text and return AST"""
        try:
            parse_tree = self.parser.parse(text)
            return self.transformer.transform(parse_tree)
        except Exception as e:
            raise ValueError(f"Parse error: {e}")

    def extract_courses(self, ast) -> List[Dict[str, Any]]:
        """Extract all courses from AST with metadata"""
        courses = []
        self._extract_courses_recursive(ast, courses, [], 0)
        return courses

    def _extract_courses_recursive(self, node, courses: List, logical_path: List, group_level: int):
        """Recursively extract courses with their logical context"""

        if isinstance(node, CourseWithMetadata):
            course_info = {
                'subject': node.course.subject,
                'number': node.course.number,
                'min_grade': node.grade.grade if node.grade else 'D',
                'help_text': node.help_text.content if node.help_text else None,
                'logical_path': logical_path.copy(),
                'group_level': group_level
            }
            courses.append(course_info)

        elif isinstance(node, AndExpression):
            for i, operand in enumerate(node.operands):
                new_path = logical_path + [('and', i)]
                self._extract_courses_recursive(
                    operand, courses, new_path, group_level)

        elif isinstance(node, OrExpression):
            for i, operand in enumerate(node.operands):
                new_path = logical_path + [('or', i)]
                self._extract_courses_recursive(
                    operand, courses, new_path, group_level)

        elif isinstance(node, GroupedExpression):
            self._extract_courses_recursive(
                node.expression, courses, logical_path, group_level + 1)


def load_courses_csv(filename='courses.csv'):
    """Load courses from CSV file"""
    try:
        df = pd.read_csv(filename)
        print(f"Loaded {len(df)} courses from {filename}")
        print(f"Columns: {list(df.columns)}")
        return df
    except FileNotFoundError:
        print(f"Error: {filename} not found in current directory")
        return None
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None


def parse_all_prerequisites():
    """Parse all prerequisites and corequisites from courses.csv"""

    # Load the CSV
    df = load_courses_csv()
    if df is None:
        return

    # Initialize parser
    parser = PrerequisiteParser()

    # Statistics
    total_courses = len(df)
    prerequisite_count = 0
    corequisite_count = 0
    successful_parses = 0
    failed_parses = 0

    print(f"\n{'='*80}")
    print("PARSING ALL COURSE PREREQUISITES AND COREQUISITES")
    print(f"{'='*80}")

    # Process each course
    for idx, row in df.iterrows():
        course_code = f"{row['subject_id']} {row['course_number']}"
        course_title = str(row['title'])[
            :50] + "..." if len(str(row['title'])) > 50 else str(row['title'])

        print(f"\n[{idx+1:4d}/{total_courses}] {course_code}: {course_title}")
        print("-" * 80)

        # Parse Prerequisites
        if pd.notna(row['prerequisites']) and str(row['prerequisites']).strip():
            prerequisite_count += 1
            prereq_text = str(row['prerequisites']).strip()

            print(f"PREREQUISITES: {prereq_text}")

            try:
                # Clean the text a bit before parsing
                cleaned_text = prereq_text.replace(
                    '\n', ' ').replace('\r', ' ')
                # Normalize whitespace
                cleaned_text = ' '.join(cleaned_text.split())

                ast = parser.parse(cleaned_text)
                courses = parser.extract_courses(ast)
                successful_parses += 1

                print(f"✅ PARSED SUCCESSFULLY ({len(courses)} courses found)")
                for i, course in enumerate(courses, 1):
                    help_text_str = f" ({
                        course['help_text']})" if course['help_text'] else ""
                    print(f"   {i}. {course['subject']} {course['number']} "
                          f"[Min Grade: {course['min_grade']}]{help_text_str}")
                    if course['logical_path'] or course['group_level'] > 0:
                        print(f"      → Logical Path: {
                              course['logical_path']}, Group Level: {course['group_level']}")

            except Exception as e:
                failed_parses += 1
                print(f"❌ PARSE FAILED: {str(e)[:100]}...")

                # Show some context for debugging
                if len(prereq_text) > 100:
                    print(f"   Text snippet: {prereq_text[:100]}...")

        # Parse Corequisites
        if pd.notna(row['corequisites']) and str(row['corequisites']).strip():
            corequisite_count += 1
            coreq_text = str(row['corequisites']).strip()

            print(f"COREQUISITES: {coreq_text}")

            try:
                # For corequisites, we'll use a simpler comma-based parsing for now
                coreq_courses = []
                for coreq in coreq_text.split(','):
                    coreq = coreq.strip()
                    if coreq:
                        # Simple parsing for corequisites (usually just comma-separated)
                        match = re.match(r'([A-Z]{2,5})\s+([A-Z0-9]+)', coreq)
                        if match:
                            coreq_courses.append({
                                'subject': match.group(1),
                                'number': match.group(2),
                                'relationship_type': 'corequisite'
                            })

                print(f"✅ COREQUISITES PARSED ({
                      len(coreq_courses)} courses found)")
                for i, course in enumerate(coreq_courses, 1):
                    print(f"   {i}. {course['subject']} {
                          course['number']} (corequisite)")

            except Exception as e:
                print(f"❌ COREQUISITE PARSE FAILED: {str(e)[:100]}")

    # Print summary statistics
    print(f"\n{'='*80}")
    print("PARSING SUMMARY")
    print(f"{'='*80}")
    print(f"Total Courses Processed: {total_courses:,}")
    print(f"Courses with Prerequisites: {prerequisite_count:,}")
    print(f"Courses with Corequisites:  {corequisite_count:,}")
    print(f"Successful Parses:       {successful_parses:,}")
    print(f"Failed Parses:           {failed_parses:,}")
    if prerequisite_count > 0:
        print(f"Success Rate:            {
              (successful_parses/prerequisite_count*100):.1f}%")


def main():
    """Main function to run the parser on courses.csv"""
    parse_all_prerequisites()


if __name__ == "__main__":
    main()
