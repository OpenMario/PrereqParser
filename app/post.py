"""
Post-processing script to replace course IDs in adjacency list with actual UUIDs from CSV
"""

import pandas as pd
import json
from typing import Dict, List, Optional, Any
import sys


class CourseIDReplacer:
    """Replaces course IDs in adjacency list with actual UUIDs from CSV"""

    def __init__(self, csv_filename: str = 'courses.csv'):
        self.csv_filename = csv_filename
        self.course_lookup = {}
        self.load_course_mapping()

    def load_course_mapping(self):
        """Load courses from CSV and create lookup mapping"""
        try:
            df = pd.read_csv(self.csv_filename)
            print(f"✅ Loaded {len(df)} courses from {self.csv_filename}")

            # Create lookup dictionary: (subject_id, course_number) -> uuid
            for _, row in df.iterrows():
                key = (row['subject_id'].strip(), row['course_number'].strip())
                self.course_lookup[key] = row['id']

            print(f"📚 Created lookup mapping for {
                  len(self.course_lookup)} courses")

        except FileNotFoundError:
            print(f"❌ Error: {self.csv_filename} not found")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            sys.exit(1)

    def find_course_uuid(self, course_name: str) -> Optional[str]:
        """Find UUID for a course given its name (e.g., 'EDUC 120')"""
        try:
            # Split course name by whitespace
            parts = course_name.strip().split()
            if len(parts) < 2:
                print(f"⚠️ Warning: Invalid course name format: '{
                      course_name}'")
                return None

            subject_id = parts[0]
            course_number = parts[1]

            # Look up in mapping
            key = (subject_id, course_number)
            if key in self.course_lookup:
                return self.course_lookup[key]
            else:
                print(f"⚠️ Warning: Course not found in CSV: {
                      subject_id} {course_number}")
                return None

        except Exception as e:
            print(f"⚠️ Error processing course name '{course_name}': {e}")
            return None

    def process_course_object(self, course: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single course object and replace its ID"""
        if 'coursename' not in course:
            print("⚠️ Warning: Course object missing 'coursename' field")
            return course

        course_name = course['coursename']
        uuid = self.find_course_uuid(course_name)

        if uuid:
            # Create updated course object
            updated_course = course.copy()
            updated_course['id'] = uuid
            return updated_course
        else:
            # Keep original if UUID not found
            return course

    def process_and_group(self, and_group: Dict[str, Any]) -> Dict[str, Any]:
        """Process an AND group and replace course IDs"""
        if 'courses' not in and_group:
            print("⚠️ Warning: AND group missing 'courses' field")
            return and_group

        updated_and_group = and_group.copy()
        updated_courses = []

        for course in and_group['courses']:
            updated_course = self.process_course_object(course)
            updated_courses.append(updated_course)

        updated_and_group['courses'] = updated_courses
        return updated_and_group

    def process_adjacency_list(self, adjacency_data: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        """Process the entire adjacency list and replace course IDs"""
        updated_adjacency = {}
        processed_courses = 0
        updated_ids = 0

        for course_id, and_groups in adjacency_data.items():
            print(f"\n🔄 Processing course: {course_id}")

            updated_and_groups = []
            for and_group in and_groups:
                updated_and_group = self.process_and_group(and_group)
                updated_and_groups.append(updated_and_group)

                # Count updated IDs
                for course in updated_and_group.get('courses', []):
                    if course.get('id') != course.get('coursename'):
                        updated_ids += 1

            updated_adjacency[course_id] = updated_and_groups
            processed_courses += 1

        print(f"\n📊 Processing Summary:")
        print(f"   • Processed {processed_courses} courses")
        print(f"   • Updated {updated_ids} course IDs")

        return updated_adjacency

    def load_adjacency_json(self, filename: str) -> Dict[str, List[Dict]]:
        """Load adjacency list from JSON file"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"✅ Loaded adjacency list from {filename}")
            return data
        except FileNotFoundError:
            print(f"❌ Error: {filename} not found")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"❌ Error: Invalid JSON in {filename}: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error loading JSON: {e}")
            sys.exit(1)

    def save_adjacency_json(self, data: Dict[str, List[Dict]], filename: str):
        """Save updated adjacency list to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"💾 Updated adjacency list saved to {filename}")
        except Exception as e:
            print(f"❌ Error saving JSON: {e}")
            sys.exit(1)

    def print_sample_changes(self, original_data: Dict, updated_data: Dict, max_samples: int = 3):
        """Print sample of changes made"""
        print(f"\n{'='*60}")
        print("📋 SAMPLE CHANGES")
        print(f"{'='*60}")

        count = 0
        for course_id in original_data:
            if count >= max_samples:
                break

            orig_groups = original_data[course_id]
            updated_groups = updated_data[course_id]

            for i, (orig_group, updated_group) in enumerate(zip(orig_groups, updated_groups)):
                orig_courses = orig_group.get('courses', [])
                updated_courses = updated_group.get('courses', [])

                for j, (orig_course, updated_course) in enumerate(zip(orig_courses, updated_courses)):
                    if orig_course.get('id') != updated_course.get('id'):
                        print(f"\nCourse: {
                              course_id} -> Group {i+1} -> Course {j+1}")
                        print(f"  Course Name: {
                              orig_course.get('coursename')}")
                        print(f"  Original ID: {orig_course.get('id')}")
                        print(f"  Updated ID:  {updated_course.get('id')}")
                        count += 1
                        break
                if count >= max_samples:
                    break
            if count >= max_samples:
                break

        if count == 0:
            print("No changes were made to course IDs")

    def process_file(self, input_filename: str, output_filename: str = None):
        """Main processing function"""
        if output_filename is None:
            output_filename = input_filename.replace('.json', '_updated.json')

        print(f"🚀 Starting Course ID Replacement Process")
        print(f"📁 Input file: {input_filename}")
        print(f"📁 Output file: {output_filename}")
        print(f"📁 CSV file: {self.csv_filename}")

        # Load original data
        original_data = self.load_adjacency_json(input_filename)

        # Process and update IDs
        updated_data = self.process_adjacency_list(original_data)

        # Show sample changes
        self.print_sample_changes(original_data, updated_data)

        # Save updated data
        self.save_adjacency_json(updated_data, output_filename)

        print(f"\n✅ Process completed successfully!")
        return updated_data


def main():
    """Main function with command line interface"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Replace course IDs in adjacency list with UUIDs from CSV')
    parser.add_argument('input_json', help='Input adjacency list JSON file')
    parser.add_argument(
        '-o', '--output', help='Output JSON file (default: input_updated.json)')
    parser.add_argument('-c', '--csv', default='courses.csv',
                        help='CSV file with course data (default: courses.csv)')

    args = parser.parse_args()

    # Create replacer and process file
    replacer = CourseIDReplacer(csv_filename=args.csv)
    replacer.process_file(args.input_json, args.output)


if __name__ == "__main__":
    # Example usage when run directly
    if len(sys.argv) > 1:
        main()
    else:
        # Demo usage
        print("Demo: Processing course_adjacency_graph.json with courses.csv")
        replacer = CourseIDReplacer('courses.csv')
        replacer.process_file('course_adjacency_graph.json',
                              'course_adjacency_graph_updated.json')
