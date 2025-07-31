import sections_202515 from "../assets/sections_202515.json" with {
  type: "json",
};
import sections_202525 from "../assets/sections_202525.json" with {
  type: "json",
};
import sections_202535 from "../assets/sections_202535.json" with {
  type: "json",
};
import sections_202545 from "../assets/sections_202545.json" with {
  type: "json",
};
import { parseArgs } from "@std/cli/parse-args";
import { neo4jService } from "./service.ts";

type RawCourseData = {
  subject_code: string;
  course_number: string;
  instruction_type: string;
  instruction_method: string;
  section: string;
  crn: number;
  enroll: string;
  max_enroll: string;
  course_title: string;
  days: string[] | null;
  start_time: string | null;
  end_time: string | null;
  instructors:
  | Array<{
    name?: string;
  }>
  | null;
  prereqs: string;
  credits: string;
};

type SectionCourseRelation = {
  instruction_type: string;
  instruction_method: string;
};

type Section = {
  term: number;
  crn: number;
  course_id: string;
  section: string;
  course: string;
  subject_code: string;
  course_number: string;
  max_enroll: string;
  start_time: string | null;
  end_time: string | null;
};

type Day = {
  id: "Monday" | "Tuesday" | "Wednesday" | "Thursday" | "Friday" | "Saturday";
};

type Term = {
  id: number;
};

const args = parseArgs(Deno.args, {
  boolean: ["prod"],
  default: {
    prod: false,
  },
});

const isProd = args.prod;
const service = await neo4jService(isProd);
await service.initialize();
console.log("after init");

// Create Day nodes first
const dayNodes: Day["id"][] = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

console.log("Creating Day nodes...");
for (const dayId of dayNodes) {
  await service.executeWriteQuery(
    `MERGE (d:Day {id: $dayId})`,
    { dayId }
  ).catch((error) => {
    console.log(`Failed to create day ${dayId}:`, error.message);
  });
}
console.log("Day nodes created");

// Create Term nodes
const termIds = [202515, 202525, 202535, 202545];

console.log("Creating Term nodes...");
for (const termId of termIds) {
  await service.executeWriteQuery(
    `MERGE (t:Term {id: $termId})`,
    { termId }
  ).catch((error) => {
    console.log(`Failed to create term ${termId}:`, error.message);
  });
}
console.log("Term nodes created");

// Get the current max instructor ID to handle auto-increment
let maxInstructorId = 0;
console.log("Getting max instructor ID...");
await service.executeReadQuery(
  `MATCH (i:Instructor) 
   RETURN COALESCE(MAX(i.id), 0) as maxId`
).then((result) => {
  maxInstructorId = result[0]?.get("maxId") || 0;
  console.log(`Current max instructor ID: ${maxInstructorId}`);
}).catch((error) => {
  console.log("Failed to get max instructor ID:", error.message);
});

// Process each term sequentially instead of using for await
const termData = [
  { sections: sections_202515, term_id: 202515 },
  { sections: sections_202525, term_id: 202525 },
  { sections: sections_202535, term_id: 202535 },
  { sections: sections_202545, term_id: 202545 },
];

for (const { sections, term_id } of termData) {
  console.log(`Processing term ${term_id} with ${sections.length} sections`);

  // Use Map instead of array - key: course string, value: course id
  const coursesMap = new Map<string, string>();

  // Use Map for instructors - key: instructor name, value: instructor id
  const instructorsMap = new Map<string, string>();

  for (const section of sections) {
    const { course_number, subject_code: subject_id, instructors } = section;
    const course = `${subject_id} ${course_number}`;

    // Lookup course if not already in map
    if (!coursesMap.has(course)) {
      await service
        .executeReadQuery(
          `MATCH (c:Course)
WHERE c.subject_id = $subject_id AND c.course_number = $course_number
RETURN c.id`,
          { subject_id, course_number }
        )
        .then((result) => {
          const id = result[0]?.get("c.id") || null;
          if (id) {
            coursesMap.set(course, id);
          } 
          return result;
        })
        .catch((error) => {
          console.log(`Failed to retrieve: ${course_number} ${subject_id}`, error.message);
          return null;
        });
    }

    // Lookup instructors if they exist
    if (instructors && Array.isArray(instructors)) {
      for (const instructor of instructors) {
        if (instructor.name) {
          const instructorName = instructor.name.trim();

          // Only lookup if not already in map
          if (!instructorsMap.has(instructorName)) {
            await service
              .executeReadQuery(
                `MATCH (i:Instructor)
WHERE i.name = $name
RETURN i.id`,
                { name: instructorName }
              )
              .then((result) => {
                const id = result[0]?.get("i.id") || null;
                if (id) {
                  instructorsMap.set(instructorName, id);
                } else {
                  // Instructor doesn't exist, create one with incremental ID
                  maxInstructorId++;
                  const newInstructorId = maxInstructorId;
                  
                  return service.executeWriteQuery(
                    `CREATE (i:Instructor {id: $id, name: $name})
                     RETURN i.id`,
                    { id: newInstructorId, name: instructorName }
                  ).then((createResult) => {
                    instructorsMap.set(instructorName, newInstructorId);
                    console.log(`Created new instructor: ${instructorName} with ID ${newInstructorId}`);
                  }).catch((createError) => {
                    console.log(`Failed to create instructor ${instructorName}:`, createError.message);
                    // Decrement back if creation failed
                    maxInstructorId--;
                  });
                }
                return result;
              })
              .catch((error) => {
                console.log(`Failed to retrieve instructor: ${instructorName}`, error.message);
                return null;
              });
          }
        }
      }
    }
  }

  console.log(`Processed ${coursesMap.size} courses and ${instructorsMap.size} instructors for term ${term_id}`);

  // Create Section nodes and relationships
  console.log(`Creating sections for term ${term_id}...`);
  for (const sectionData of sections) {
    const { 
      subject_code, 
      course_number, 
      instruction_type, 
      instruction_method,
      section,
      crn,
      max_enroll,
      start_time,
      end_time,
      days
    } = sectionData;

    const course = `${subject_code} ${course_number}`;
    const course_id = coursesMap.get(course);

    // Only create section if we found the corresponding course
    if (course_id) {
      const sectionNode: Section = {
        term: term_id,
        crn,
        course_id,
        section,
        course,
        subject_code,
        course_number,
        max_enroll,
        start_time,
        end_time
      };

      const relationshipProps: SectionCourseRelation = {
        instruction_type,
        instruction_method
      };

      await service.executeWriteQuery(
          `MATCH (c:Course {id: $course_id})
           MATCH (t:Term {id: $term})
           MERGE (s:Section {crn: $crn})
           SET s.term = $term,
               s.course_id = $course_id,
               s.section = $section,
               s.course = $course,
               s.subject_code = $subject_code,
               s.course_number = $course_number,
               s.max_enroll = $max_enroll,
               s.start_time = $start_time,
               s.end_time = $end_time
           MERGE (c)-[:OFFERS {
             instruction_type: $instruction_type,
             instruction_method: $instruction_method
           }]->(s)
           MERGE (s)-[:OFFERED_ON]->(t)`,
        {
          course_id: sectionNode.course_id,
          term: sectionNode.term,
          crn: sectionNode.crn,
          section: sectionNode.section,
          course: sectionNode.course,
          subject_code: sectionNode.subject_code,
          course_number: sectionNode.course_number,
          max_enroll: sectionNode.max_enroll,
          start_time: sectionNode.start_time,
          end_time: sectionNode.end_time,
          instruction_type: relationshipProps.instruction_type,
          instruction_method: relationshipProps.instruction_method
        }
      ).catch((error: any) => {
        console.log(`Failed to create section ${crn} for course ${course}:`, error.message);
      });

      // Create SCHEDULED_ON relationships with Day nodes
      if (days && Array.isArray(days) && days.length > 0) {
        for (const day of days) {
          // Normalize day name to match our Day type
          const normalizedDay = day.trim();
          
          // Only create relationship if it's a valid day
          if (dayNodes.includes(normalizedDay as Day["id"])) {
            await service.executeWriteQuery(
              `MATCH (s:Section {crn: $crn})
               MATCH (d:Day {id: $dayId})
               MERGE (s)-[:SCHEDULED_ON]->(d)`,
              { crn, dayId: normalizedDay }
            ).catch((error: any) => {
              console.log(`Failed to create SCHEDULED_ON relationship for section ${crn} and day ${normalizedDay}:`, error.message);
            });
          } else {
            console.log(`Invalid day found for section ${crn}: ${day}`);
          }
        }
      }

      // Create instructor relationships
      if (sectionData.instructors && Array.isArray(sectionData.instructors)) {
        for (const instructor of sectionData.instructors) {
          if (instructor.name) {
            const instructorName = instructor.name.trim();
            const instructor_id = instructorsMap.get(instructorName);

            if (instructor_id) {
              // Create TEACHES relationship (Instructor -> Section)
              await service.executeWriteQuery(
                `MATCH (i:Instructor {id: $instructor_id})
                 MATCH (s:Section {crn: $crn})
                 MERGE (i)-[:TEACHES]->(s)`,
                { instructor_id, crn }
              ).catch((error: any) => {
                console.log(`Failed to create TEACHES relationship for instructor ${instructorName} and section ${crn}:`, error.message);
              });

              // Create TAUGHT relationship (Instructor -> Course)
              await service.executeWriteQuery(
                `MATCH (i:Instructor {id: $instructor_id})
                 MATCH (c:Course {id: $course_id})
                 MERGE (i)-[:TAUGHT]->(c)`,
                { instructor_id, course_id }
              ).catch((error: any) => {
                console.log(`Failed to create TAUGHT relationship for instructor ${instructorName} and course ${course}:`, error.message);
              });
            } else {
              console.log(`Instructor not found in map: ${instructorName} for section ${crn}`);
            }
          }
        }
      }
    } else {
      // console.log(`Skipping section ${crn} - course ${course} not found`);
    }
  }

  console.log(`Completed creating sections for term ${term_id}`);
}

// Don't forget to close the service connection
await service.close();
console.log("Processing complete");
