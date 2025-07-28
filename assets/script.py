import json
import pandas as pd
import re


def create_coreq_mapping(csv_file):
    df = pd.read_csv(csv_file)

    # Create a mapping of subject_id + course_number to id for quick lookup
    course_lookup = {}
    for _, row in df.iterrows():
        key = f"{row['subject_id']}{row['course_number']}"
        course_lookup[key] = row['id']

    coreq_mapping = {}

    for _, row in df.iterrows():
        course_id = row['id']
        coreq_text = row['corequisites']

        # Skip if no corequisites
        if pd.isna(coreq_text) or not coreq_text.strip():
            continue

        # Extract course codes from corequisites text
        # This regex looks for patterns like "MATH 101", "CS 250", etc.
        course_pattern = r'([A-Z]{2,4})\s*(\d{3,4})'
        matches = re.findall(course_pattern, coreq_text)

        coreq_ids = []
        for subject, number in matches:
            lookup_key = f"{subject}{number}"
            if lookup_key in course_lookup:
                coreq_ids.append(course_lookup[lookup_key])

        # Only add to mapping if we found valid corequisite IDs
        if coreq_ids:
            coreq_mapping[course_id] = coreq_ids

    return coreq_mapping


# Usage
mapping = create_coreq_mapping('./courses.csv')
print(mapping)

# Pretty print
print(json.dumps(mapping, indent=2))
