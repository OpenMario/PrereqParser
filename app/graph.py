"""
Course Prerequisite to JSON Adjacency Graph Converter
Uses the existing PrerequisiteParser to generate adjacency graph from CSV data
"""

import pandas as pd
import json
from typing import Dict, List, Any
from parser import (
    PrerequisiteParser,
    CourseWithMetadata,
    CommaExpression,
    OrExpression,
    AndExpression,
    GroupedExpression
)


class AdjacencyGraphGenerator:
    """Converts parsed prerequisites to JSON adjacency graph format"""

    def __init__(self):
        self.parser = PrerequisiteParser()

    def load_courses_csv(self, filename: str = 'courses.csv') -> pd.DataFrame:
        """Load courses from CSV file"""
        try:
            df = pd.read_csv(filename)
            print(f"✅ Loaded {len(df)} courses from {filename}")
            return df
        except FileNotFoundError:
            print(f"❌ Error: {filename} not found")
            return None
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return None

    def convert_ast_to_graph_format(self, ast) -> List[Dict[str, Any]]:
        """Convert AST to the specific adjacency graph format"""
        and_groups = []
        self._process_node_for_adjacency(ast, and_groups)
        return and_groups

    def _process_node_for_adjacency(self, node, and_groups: List):
        """Process AST node and build AND groups with OR courses"""

        if isinstance(node, CourseWithMetadata):
            # Single course - create a new AND group with this course
            course_info = {
                'coursename': f"{node.course.subject} {node.course.number}",
                'id': f"{node.course.subject} {node.course.number}",
                'minimum_grade': node.grade.grade if node.grade else 'D'
            }

            and_group = {
                'courses': [course_info],
                'helpertext': node.help_text.content if node.help_text else ''
            }
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

    def _process_or_group(self, operands: List, and_groups: List):
        """Process a list of operands that should be ORed together"""
        or_courses = []
        combined_helper_text = ''

        for operand in operands:
            if isinstance(operand, CourseWithMetadata):
                course_info = {
                    'coursename': f"{operand.course.subject} {operand.course.number}",
                    'id': f"{operand.course.subject} {operand.course.number}",
                    'minimum_grade': operand.grade.grade if operand.grade else 'D'
                }
                or_courses.append(course_info)

                if operand.help_text and not combined_helper_text:
                    combined_helper_text = operand.help_text.content
            else:
                # For complex nested structures, recursively process
                temp_groups = []
                self._process_node_for_adjacency(operand, temp_groups)
                # Flatten into or_courses if possible
                for group in temp_groups:
                    or_courses.extend(group['courses'])
                    if group['helpertext'] and not combined_helper_text:
                        combined_helper_text = group['helpertext']

        if or_courses:
            and_group = {
                'courses': or_courses,
                'helpertext': combined_helper_text
            }
            and_groups.append(and_group)

    def generate_adjacency_graph(self, csv_filename: str = 'courses.csv') -> Dict[str, List]:
        """Generate complete adjacency graph from CSV"""

        # Load CSV data
        df = self.load_courses_csv(csv_filename)
        if df is None:
            return {}

        # Filter courses with prerequisites
        courses_with_prereqs = df[df['prerequisites'].notna() & (
            df['prerequisites'] != '')]
        print(f"🔍 Found {len(courses_with_prereqs)
                         } courses with prerequisites")

        adjacency_graph = {}
        successful_parses = 0
        failed_parses = 0

        for idx, row in courses_with_prereqs.iterrows():
            course_id = row['id']
            course_name = f"{row['subject_id']} {row['course_number']}"
            prereq_text = str(row['prerequisites']).strip()

            print(f"\n📚 Processing: {course_name}")
            print(f"   Prerequisites: {prereq_text}")

            try:
                # Parse using the existing parser
                ast = self.parser.parse(prereq_text)

                # Convert to adjacency graph format
                and_groups = self.convert_ast_to_graph_format(ast)

                if and_groups:
                    adjacency_graph[course_id] = and_groups
                    successful_parses += 1
                    print(
                        f"   ✅ Successfully parsed - {len(and_groups)} AND group(s)")

                    # Show the groups
                    for i, group in enumerate(and_groups):
                        if len(group['courses']) == 1:
                            print(f"      Group {
                                  i+1}: Requires {group['courses'][0]['coursename']}")
                        else:
                            course_names = [c['coursename']
                                            for c in group['courses']]
                            print(f"      Group {
                                  i+1}: Choose one of: {', '.join(course_names)}")
                        if group['helpertext']:
                            print(f"         Note: {group['helpertext']}")

            except Exception as e:
                failed_parses += 1
                print(f"   ❌ Parse failed: {str(e)[:100]}")

        # Print summary
        print(f"\n{'='*60}")
        print("📊 SUMMARY")
        print(f"{'='*60}")
        print(f"Total courses processed: {len(courses_with_prereqs)}")
        print(f"Successful parses: {successful_parses}")
        print(f"Failed parses: {failed_parses}")
        if len(courses_with_prereqs) > 0:
            print(f"Success rate: {
                  (successful_parses/len(courses_with_prereqs)*100):.1f}%")

        return adjacency_graph

    def save_to_json(self, adjacency_graph: Dict, filename: str = 'course_adjacency_graph.json'):
        """Save adjacency graph to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(adjacency_graph, f, indent=2, ensure_ascii=False)
            print(f"💾 Adjacency graph saved to {filename}")
        except Exception as e:
            print(f"❌ Error saving to JSON: {e}")

    def print_sample_output(self, adjacency_graph: Dict, max_samples: int = 3):
        """Print sample output for verification"""
        print(f"\n{'='*60}")
        print("📋 SAMPLE OUTPUT")
        print(f"{'='*60}")

        count = 0
        for course_id, groups in adjacency_graph.items():
            if count >= max_samples:
                break

            print(f"\nCourse ID: {course_id}")
            print(json.dumps(groups, indent=2))
            count += 1

        if len(adjacency_graph) > max_samples:
            print(f"\n... and {len(adjacency_graph) -
                  max_samples} more courses")
