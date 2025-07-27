"""
Course Prerequisite Parser using Lark
Install: pip install lark pandas
"""

import pandas as pd
from parser import PrerequisiteParser


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
    # test_comma_parsing()

    # Then run the full parsing
    parse_all_prerequisites()


if __name__ == "__main__":
    main()
