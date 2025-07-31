"""
Neo4j Prerequisite Relationship Loader
Loads JSON prerequisite data and creates PREREQUISITE relationships in Neo4j
"""

import json
from typing import Dict, List
from neo4j import GraphDatabase
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Neo4jPrerequisiteLoader:
    def __init__(self, uri: str, username: str, password: str):
        """Initialize Neo4j connection"""
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        logger.info(f"Connected to Neo4j at {uri}")

    def close(self):
        """Close Neo4j connection"""
        self.driver.close()

    def load_json_data(self, filename: str) -> Dict[str, List[Dict]]:
        """Load prerequisite data from JSON file"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} courses from {filename}")
            return data
        except FileNotFoundError:
            logger.error(f"File {filename} not found")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {filename}: {e}")
            return {}

    def clear_prerequisite_relationships(self):
        """Remove all existing PREREQUISITE relationships"""
        with self.driver.session() as session:
            result = session.run(
                "MATCH ()-[r:PREREQUISITE]->() DELETE r RETURN count(r) as deleted")
            deleted_count = result.single()["deleted"]
            logger.info(
                f"Deleted {deleted_count} existing PREREQUISITE relationships")

    def create_prerequisite_relationships(self, prereq_data: Dict[str, List[Dict]]):
        """Create all PREREQUISITE relationships from JSON data"""
        total_relationships = 0

        with self.driver.session() as session:
            for target_course_id, and_groups in prereq_data.items():
                logger.info(f"Processing course: {target_course_id}")

                for group_index, and_group in enumerate(and_groups):
                    group_id = f"g{group_index + 1}"
                    courses = and_group.get("courses", [])
                    can_take_concurrent = and_group.get(
                        "canBeTakenConcurrently", False)

                    # Determine relationship type
                    relationship_type = "CHOICE" if len(
                        courses) > 1 else "REQUIRED"

                    logger.info(f"  Group {group_id}: {
                                relationship_type} - {len(courses)} courses")

                    for course in courses:
                        prereq_name = course.get("coursename")
                        prereq_id = course.get("id")
                        minimum_grade = course.get("minimum_grade", "D")

                        # Create the relationship
                        relationship_created = self._create_single_relationship(
                            session,
                            prereq_name,
                            prereq_id,
                            target_course_id,
                            group_id,
                            relationship_type,
                            minimum_grade,
                            can_take_concurrent
                        )

                        if relationship_created:
                            total_relationships += 1
                            logger.info(
                                f"    ✅ {prereq_name} -> {target_course_id}")
                        else:
                            logger.warning(f"    ❌ Failed: {
                                           prereq_name} -> {target_course_id}")

        logger.info(
            f"Created {total_relationships} PREREQUISITE relationships")

    def _create_single_relationship(self, session, prereq_name: str, prereq_id: str,
                                    target_id: str, group_id: str, relationship_type: str,
                                    minimum_grade: str, can_take_concurrent: bool) -> bool:
        """Create a single PREREQUISITE relationship"""

        # First try to match by course name, then by ID
        cypher_query = """
        // Try to find prerequisite course by name first, then by ID
        OPTIONAL MATCH (prereq:Course) WHERE prereq.name = $prereq_name OR prereq.id = $prereq_id
        OPTIONAL MATCH (target:Course) WHERE target.id = $target_id
        
        WITH prereq, target
        WHERE prereq IS NOT NULL AND target IS NOT NULL
        
        CREATE (prereq)-[:PREREQUISITE {
            group_id: $group_id,
            relationship_type: $relationship_type,
            minimum_grade: $minimum_grade,
            can_take_concurrent: $can_take_concurrent
        }]->(target)
        
        RETURN prereq.name as prereq_name, target.name as target_name
        """

        try:
            result = session.run(cypher_query, {
                "prereq_name": prereq_name,
                "prereq_id": prereq_id,
                "target_id": target_id,
                "group_id": group_id,
                "relationship_type": relationship_type,
                "minimum_grade": minimum_grade,
                "can_take_concurrent": can_take_concurrent
            })

            record = result.single()
            return record is not None

        except Exception as e:
            logger.error(f"Error creating relationship {
                         prereq_name} -> {target_id}: {e}")
            return False

    def verify_relationships(self) -> Dict[str, int]:
        """Verify the created relationships"""
        with self.driver.session() as session:
            # Count total relationships
            result = session.run(
                "MATCH ()-[r:PREREQUISITE]->() RETURN count(r) as total")
            total = result.single()["total"]

            # Count by relationship type
            result = session.run("""
                MATCH ()-[r:PREREQUISITE]->() 
                RETURN r.relationship_type as type, count(r) as count
                ORDER BY type
            """)
            by_type = {record["type"]: record["count"] for record in result}

            # Count by group
            result = session.run("""
                MATCH ()-[r:PREREQUISITE]->() 
                RETURN r.group_id as group_id, count(r) as count
                ORDER BY group_id
                LIMIT 10
            """)
            by_group = {record["group_id"]: record["count"]
                        for record in result}

            stats = {
                "total_relationships": total,
                "by_type": by_type,
                "sample_groups": by_group
            }

            logger.info(f"Verification - Total relationships: {total}")
            logger.info(f"By type: {by_type}")

            return stats

    def get_sample_course_prerequisites(self, course_id: str) -> List[Dict]:
        """Get prerequisites for a sample course"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (prereq:Course)-[r:PREREQUISITE]->(target:Course {id: $course_id})
                RETURN r.group_id as group_id, 
                       r.relationship_type as relationship_type,
                       collect({
                           name: prereq.name, 
                           id: prereq.id,
                           minimum_grade: r.minimum_grade,
                           can_take_concurrent: r.can_take_concurrent
                       }) as prerequisites
                ORDER BY r.group_id
            """, {"course_id": course_id})

            return [record.data() for record in result]


def main():
    """Main function to load and process prerequisite data"""

    NEO4J_URI = "neo4j://127.0.0.1:7687"  # Updated to match your Neo4j Desktop
    NEO4J_USERNAME = "neo4j"              # Default username
    NEO4J_PASSWORD = "321321654321"           # Change to your actual password
    JSON_FILENAME = "./out/deps_graph.json"  # Your JSON fill

    # Initialize loader
    loader = Neo4jPrerequisiteLoader(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)

    try:
        # Load JSON data
        prereq_data = loader.load_json_data(JSON_FILENAME)

        if not prereq_data:
            logger.error("No data loaded. Exiting.")
            return

        # Clear existing relationships (optional)
        clear_existing = input(
            "Clear existing PREREQUISITE relationships? (y/N): ").lower()
        if clear_existing == 'y':
            loader.clear_prerequisite_relationships()

        # Create new relationships
        logger.info("Creating PREREQUISITE relationships...")
        loader.create_prerequisite_relationships(prereq_data)

        # Verify results
        stats = loader.verify_relationships()

        # Show sample
        if prereq_data:
            sample_course_id = list(prereq_data.keys())[0]
            logger.info(f"\nSample prerequisites for course {
                        sample_course_id}:")
            sample_prereqs = loader.get_sample_course_prerequisites(
                sample_course_id)
            for group in sample_prereqs:
                print(f"  Group {group['group_id']} ({group['relationship_type']}): {
                      [p['name'] for p in group['prerequisites']]}")

        logger.info("✅ Prerequisite loading completed successfully!")

    except Exception as e:
        logger.error(f"Error during loading: {e}")

    finally:
        loader.close()


if __name__ == "__main__":
    main()
