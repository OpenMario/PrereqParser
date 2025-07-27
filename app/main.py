import json
from graph import AdjacencyGraphGenerator


def main():
    """Main function to generate adjacency graph"""
    generator = AdjacencyGraphGenerator()

    # Generate the adjacency graph
    adjacency_graph = generator.generate_adjacency_graph('courses.csv')

    # Print sample output
    # generator.print_sample_output(adjacency_graph)

    # Save to JSON file
    generator.save_to_json(adjacency_graph)

    # Print the complete graph
    print(f"\n{'='*60}")
    print("🌐 COMPLETE ADJACENCY GRAPH JSON")
    print(f"{'='*60}")
    # print(json.dumps(adjacency_graph.to_dict(), indent=2))


if __name__ == "__main__":
    main()
