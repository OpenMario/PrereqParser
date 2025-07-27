"""
Course Prerequisite Parser using Lark
Install: pip install lark pandas
"""

from lark import Lark, Transformer
from typing import List, Dict, Any, Optional
import pandas as pd


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
                ast = parser.parse(prereq_text)
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
                error_msg = str(e)
                print(f"❌ PARSE FAILED: {error_msg[:100]}...")

                # Show some context for debugging
                if len(prereq_text) > 100:
                    print(f"   Text snippet: {prereq_text[:100]}...")

                # Provide specific guidance for common issues
                if "Unexpected token" in error_msg and "CLOSE_PAREN" in error_msg:
                    print(f"   → Likely unbalanced parentheses issue")
                elif "Unexpected token" in error_msg and "COURSE_NUMBER" in error_msg:
                    print(f"   → Likely malformed course code (check spacing)")
                elif "Unexpected token" in error_msg:
                    print(f"   → Check for invalid characters or formatting")

                # Show character-by-character breakdown for complex cases
                if "(" in prereq_text or ")" in prereq_text:
                    open_count = prereq_text.count('(')
                    close_count = prereq_text.count(')')
                    print(f"   → Parentheses: {
                          open_count} open, {close_count} close")

                # Check for suspicious course code patterns
                import re
                suspicious_courses = re.findall(
                    r'\b[A-Z]+\d+\s+[A-Z0-9]\b', prereq_text)
                if suspicious_courses:
                    print(f"   → Suspicious course codes found: {
                          suspicious_courses}")

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


def test_comma_parsing():
    """Test function specifically for comma parsing"""
    parser = PrerequisiteParser()

    test_cases = [
        "CHEM 253 [Min Grade: D], ENGR 210 [Min Grade: D] (Can be taken Concurrently)",
        "MATH 101, MATH 102, MATH 103",
        "CHEM 253 [Min Grade: D], ENGR 210 [Min Grade: D] and BIO 201 [Min Grade: D]",
        "A 100, B 200 or C 300, D 400",
        # Test unbalanced parentheses (the problematic case)
        "CHEM 253 [Min Grade: D] (Can be taken Concurrently) and CHEM 230 [Min Grade: D] and (CHEM 248 [Min Grade: D] or CHEM 242 [Min Grade: D]) or CHEC 352 [Min Grade: D])",
        # Test malformed course codes
        "(APPH50 P or PHYS 100 [Min Grade: D] or APC 070) and (MATH 121 [Min Grade: C-] or MATH 117 [Min Grade: C-])",
        # Test other edge cases
        "A 100 (note) and (B 200 or C 300",  # Missing closing paren
        "A 100) and B 200",  # Extra closing paren at start
        "MATH100 A and PHYS200 B",  # More malformed course codes
    ]

    print("\n" + "="*80)
    print("TESTING COMMA PARSING")
    print("="*80)

    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest {i}: {test_case}")
        print("-" * 60)
        try:
            ast = parser.parse(test_case)
            courses = parser.extract_courses(ast)
            print(f"✅ SUCCESS - Found {len(courses)} courses:")
            for j, course in enumerate(courses, 1):
                logical_path_str = f" → {
                    course['logical_path']}" if course['logical_path'] else ""
                print(f"   {j}. {course['subject']} {
                      course['number']} [Min Grade: {course['min_grade']}]{logical_path_str}")
            print(f"AST: {ast}")
        except Exception as e:
            print(f"❌ FAILED: {e}")


def main():
    """Main function to run the parser on courses.csv"""
    # First test comma parsing
    test_comma_parsing()

    # Then run the full parsing
    parse_all_prerequisites()


if __name__ == "__main__":
    main()
