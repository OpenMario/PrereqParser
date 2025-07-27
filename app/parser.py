from lark import Lark, Transformer
from typing import List, Dict, Any, Optional

PREREQUISITE_GRAMMAR = r"""
    start: or_expression

    ?or_expression: and_expression
                  | or_expression "or"i and_expression

    ?and_expression: comma_expression
                   | and_expression "and"i comma_expression

    ?comma_expression: course_term
                     | comma_expression "," course_term

    ?course_term: course_with_metadata
                | "(" or_expression ")"  -> grouped_expression

    course_with_metadata: course_code grade_requirement? help_text?

    course_code: SUBJECT_ID COURSE_NUMBER

    grade_requirement: "[" MIN_GRADE_TEXT GRADE_VALUE "]"

    help_text: OPEN_PAREN HELP_TEXT_CONTENT CLOSE_PAREN

    OPEN_PAREN: "("
    CLOSE_PAREN: ")"
    HELP_TEXT_CONTENT: /[^)]+/
    SUBJECT_ID: /[A-Z0-9]{2,5}/
    COURSE_NUMBER: /[A-Z0-9]+/
    GRADE_VALUE: /CR|NC|[A-F][+-]?/
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


class CommaExpression:
    def __init__(self, operands: List[Any]):
        self.operands = operands

    def __repr__(self):
        return f"CommaExpression({self.operands})"


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

    def comma_expression(self, args):
        if len(args) == 1:
            return args[0]  # Single term, no need to wrap
        return CommaExpression(args)

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

    def preprocess_text(self, text: str) -> str:
        """Preprocess text to handle common data quality issues"""
        # Clean whitespace
        text = text.replace('\n', ' ').replace('\r', ' ')
        text = ' '.join(text.split())

        # Fix malformed course codes
        text = self._fix_course_codes(text)

        # Fix unbalanced parentheses
        text = self._fix_unbalanced_parentheses(text)

        return text

    def _fix_course_codes(self, text: str) -> str:
        """Fix common course code formatting issues"""
        import re

        # Pattern to match malformed course codes like "APPH50 P" or "MATH100 A"
        # This matches: letters+numbers followed by space and single letter/number
        pattern = r'\b([A-Z]+)(\d+)\s+([A-Z0-9])\b'

        def replace_course_code(match):
            subject_part = match.group(1)  # "APPH"
            number_part = match.group(2)   # "50"
            suffix_part = match.group(3)   # "P"

            # Try to determine if this should be "SUBJ NUMP" or "SUBJNUM P"
            if len(subject_part) <= 4:  # Likely a normal subject code
                fixed = f"{subject_part} {number_part}{suffix_part}"
                print(f"   WARNING: Fixed course code '{
                      match.group(0)}' → '{fixed}'")
                return fixed
            else:  # Subject might include numbers
                fixed = f"{
                    subject_part[:-len(number_part)]} {number_part}{suffix_part}"
                print(f"   WARNING: Fixed course code '{
                      match.group(0)}' → '{fixed}'")
                return fixed

        original_text = text
        text = re.sub(pattern, replace_course_code, text)

        return text

    def _fix_unbalanced_parentheses(self, text: str) -> str:
        """Fix unbalanced parentheses by removing extras"""
        open_count = 0
        fixed_chars = []

        # First pass: track opens and mark excess closes
        for char in text:
            if char == '(':
                open_count += 1
                fixed_chars.append(char)
            elif char == ')':
                if open_count > 0:
                    open_count -= 1
                    fixed_chars.append(char)
                else:
                    # Skip excess closing parenthesis
                    print(f"   WARNING: Removed excess closing parenthesis")
                    continue
            else:
                fixed_chars.append(char)

        # Remove any unclosed opening parentheses from the end
        if open_count > 0:
            print(f"   WARNING: Found {
                  open_count} unclosed opening parentheses")
            # For now, just add closing parens at the end
            fixed_chars.extend([')'] * open_count)

        return ''.join(fixed_chars)

    def parse(self, text: str):
        """Parse prerequisite text and return AST"""
        try:
            # Preprocess text to handle common issues
            cleaned_text = self.preprocess_text(text)
            if cleaned_text != text:
                print(f"   TEXT CLEANED: {text}")
                print(f"   TO: {cleaned_text}")

            parse_tree = self.parser.parse(cleaned_text)
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

        elif isinstance(node, CommaExpression):
            for i, operand in enumerate(node.operands):
                new_path = logical_path + [('comma', i)]
                self._extract_courses_recursive(
                    operand, courses, new_path, group_level)

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
