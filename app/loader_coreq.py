"""
Neo4j Corequisite Relationship Loader
Loads JSON corequisite data and creates COREQUISITE relationships in Neo4j
"""

import json
from typing import Dict, List
from neo4j import GraphDatabase
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Neo4jCorequisiteLoader:
    def __init__(self, uri: str, username: str, password: str):
        """Initialize Neo4j connection"""
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        logger.info(f"Connected to Neo4j at {uri}")

    def close(self):
        """Close Neo4j connection"""
        self.driver.close()

    def load_json_data(self, filename: str) -> Dict[str, List[str]]:
        """Load corequisite data from JSON file"""
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

    def clear_corequisite_relationships(self):
        """Remove all existing COREQUISITE relationships"""
        with self.driver.session() as session:
            result = session.run(
                "MATCH ()-[r:COREQUISITE]->() DELETE r RETURN count(r) as deleted")
            deleted_count = result.single()["deleted"]
            logger.info(
                f"Deleted {deleted_count} existing COREQUISITE relationships")

    def create_corequisite_relationships(self, coreq_data: Dict[str, List[str]]):
        """Create all COREQUISITE relationships from JSON data"""
        total_relationships = 0

        with self.driver.session() as session:
            for course_id, corequisite_ids in coreq_data.items():
                logger.info(f"Processing course: {course_id}")
                logger.info(f"  Corequisites: {len(corequisite_ids)} courses")

                for coreq_id in corequisite_ids:
                    # Create bidirectional corequisite relationships
                    # Course A -> Course B (corequisite)
                    relationship_created_1 = self._create_single_relationship(
                        session, course_id, coreq_id
                    )

                    # Course B -> Course A (corequisite)
                    relationship_created_2 = self._create_single_relationship(
                        session, coreq_id, course_id
                    )

                    if relationship_created_1:
                        total_relationships += 1
                        logger.info(
                            f"    ✅ {course_id} <-> {coreq_id} (forward)")
                    else:
                        logger.warning(f"    ❌ Failed: {
                                       course_id} -> {coreq_id}")

                    if relationship_created_2:
                        total_relationships += 1
                        logger.info(
                            f"    ✅ {coreq_id} <-> {course_id} (reverse)")
                    else:
                        logger.warning(f"    ❌ Failed: {
                                       coreq_id} -> {course_id}")

        logger.info(f"Created {total_relationships} COREQUISITE relationships")

    def _create_single_relationship(self, session, from_course_id: str, to_course_id: str) -> bool:
        """Create a single COREQUISITE relationship"""

        cypher_query = """
        // Find both courses by ID
        MATCH (from_course:Course {id: $from_course_id})
        MATCH (to_course:Course {id: $to_course_id})
        
        // Check if relationship already exists to avoid duplicates
        OPTIONAL MATCH (from_course)-[existing:COREQUISITE]->(to_course)
        
        WITH from_course, to_course, existing
        WHERE existing IS NULL  // Only create if relationship doesn't exist
        
        CREATE (from_course)-[:COREQUISITE {
            relationship_type: "COREQUISITE",
            created_at: datetime()
        }]->(to_course)
        
        RETURN from_course.id as from_id, to_course.id as to_id
        """

        try:
            result = session.run(cypher_query, {
                "from_course_id": from_course_id,
                "to_course_id": to_course_id
            })

            record = result.single()
            return record is not None

        except Exception as e:
            logger.error(f"Error creating relationship {
                         from_course_id} -> {to_course_id}: {e}")
            return False

    def verify_relationships(self) -> Dict[str, int]:
        """Verify the created relationships"""
        with self.driver.session() as session:
            # Count total relationships
            result = session.run(
                "MATCH ()-[r:COREQUISITE]->() RETURN count(r) as total")
            total = result.single()["total"]

            # Count bidirectional pairs (should be even number)
            result = session.run("""
                MATCH (a:Course)-[r1:COREQUISITE]->(b:Course)
                MATCH (b)-[r2:COREQUISITE]->(a)
                RETURN count(DISTINCT [a.id, b.id]) as bidirectional_pairs
            """)
            bidirectional_pairs = result.single()["bidirectional_pairs"]

            # Sample some relationships
            result = session.run("""
                MATCH (from:Course)-[r:COREQUISITE]->(to:Course)
                RETURN from.id as from_id, to.id as to_id, from.name as from_name, to.name as to_name
                LIMIT 10
            """)
            sample_relationships = [record.data() for record in result]

            stats = {
                "total_relationships": total,
                "bidirectional_pairs": bidirectional_pairs,
                "sample_relationships": sample_relationships
            }

            logger.info(f"Verification - Total relationships: {total}")
            logger.info(f"Bidirectional pairs: {bidirectional_pairs}")

            return stats

    def get_course_corequisites(self, course_id: str) -> List[Dict]:
        """Get corequisites for a specific course"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (course:Course {id: $course_id})-[r:COREQUISITE]->(coreq:Course)
                RETURN coreq.id as coreq_id, 
                       coreq.name as coreq_name,
                       r.created_at as created_at
                ORDER BY coreq.name
            """, {"course_id": course_id})

            return [record.data() for record in result]

    def find_mutual_corequisites(self) -> List[Dict]:
        """Find courses that have mutual corequisite relationships"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (a:Course)-[:COREQUISITE]->(b:Course)
                MATCH (b)-[:COREQUISITE]->(a)
                WHERE a.id < b.id  // Avoid duplicates
                RETURN a.id as course_a_id, a.name as course_a_name,
                       b.id as course_b_id, b.name as course_b_name
                ORDER BY a.name, b.name
                LIMIT 20
            """)

            return [record.data() for record in result]


def main():
    """Main function to load and process corequisite data"""

    NEO4J_URI = "neo4j://127.0.0.1:7687"  # Updated to match your Neo4j Desktop
    NEO4J_USERNAME = "neo4j"              # Default username
    NEO4J_PASSWORD = "321321654321"           # Change to your actual password
    JSON_FILENAME = "./out/coreqs.json"  # Your JSON file

    # Initialize loader
    loader = Neo4jCorequisiteLoader(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)

    try:
        # Load JSON data
        coreq_data = loader.load_json_data(JSON_FILENAME)

        if not coreq_data:
            logger.error("No data loaded. Exiting.")
            return

        # Clear existing relationships (optional)
        clear_existing = input(
            "Clear existing COREQUISITE relationships? (y/N): ").lower()
        if clear_existing == 'y':
            loader.clear_corequisite_relationships()

        # Create new relationships
        logger.info("Creating COREQUISITE relationships...")
        loader.create_corequisite_relationships(coreq_data)

        # Verify results
        stats = loader.verify_relationships()

        # Show sample corequisites
        if coreq_data:
            sample_course_id = list(coreq_data.keys())[0]
            logger.info(f"\nSample corequisites for course {
                        sample_course_id}:")
            sample_coreqs = loader.get_course_corequisites(sample_course_id)
            for coreq in sample_coreqs:
                print(f"  - {coreq['coreq_name']} (ID: {coreq['coreq_id']})")

        # Show mutual corequisites
        logger.info("\nSample mutual corequisite relationships:")
        mutual_coreqs = loader.find_mutual_corequisites()
        for pair in mutual_coreqs[:5]:  # Show first 5
            print(f"  {pair['course_a_name']} <-> {pair['course_b_name']}")

        logger.info("✅ Corequisite loading completed successfully!")

    except Exception as e:
        logger.error(f"Error during loading: {e}")

    finally:
        loader.close()


if __name__ == "__main__":
    main()
