import neo4j, { Driver, Session, Result, Record, auth } from 'neo4j-driver';

interface DatabaseConfig {
  uri: string;
  username: string;
  password: string;
}

interface NodeData {
  nodeId: number;
  labels: string[];
  properties: Record<string, any>;
}

interface RelationshipData {
  startId: number;
  endId: number;
  type: string;
  properties: Record<string, any>;
}

interface ReplicationProgress {
  totalNodes: number;
  processedNodes: number;
  totalRelationships: number;
  processedRelationships: number;
  stage: 'clearing' | 'schema' | 'nodes' | 'relationships' | 'cleanup' | 'complete';
}

export class DatabaseReplicator {
  private sourceDriver: Driver;
  private targetDriver: Driver;
  private readonly batchSize: number;

  constructor(
    sourceConfig: DatabaseConfig,
    targetConfig: DatabaseConfig,
    batchSize: number = 1000
  ) {
    this.sourceDriver = neo4j.driver(
      sourceConfig.uri,
      auth.basic(sourceConfig.username, sourceConfig.password)
    );
    this.targetDriver = neo4j.driver(
      targetConfig.uri,
      auth.basic(targetConfig.username, targetConfig.password)
    );
    this.batchSize = batchSize;
  }

  async replicateDatabase(
    progressCallback?: (progress: ReplicationProgress) => void
  ): Promise<void> {
    try {
      console.log('Starting database replication...');
      
      // Get total counts for progress tracking
      const { totalNodes, totalRelationships } = await this.getTotalCounts();
      
      let progress: ReplicationProgress = {
        totalNodes,
        processedNodes: 0,
        totalRelationships,
        processedRelationships: 0,
        stage: 'clearing'
      };

      progressCallback?.(progress);

      // Step 1: Clear target database
      await this.clearTargetDatabase();
      progress.stage = 'schema';
      progressCallback?.(progress);

      // Step 2: Copy schema (constraints and indexes)
      await this.copySchema();
      progress.stage = 'nodes';
      progressCallback?.(progress);

      // Step 3: Copy nodes
      await this.copyNodes((processedNodes) => {
        progress.processedNodes = processedNodes;
        progressCallback?.(progress);
      });
      progress.stage = 'relationships';
      progressCallback?.(progress);

      // Step 4: Copy relationships
      await this.copyRelationships((processedRels) => {
        progress.processedRelationships = processedRels;
        progressCallback?.(progress);
      });
      progress.stage = 'cleanup';
      progressCallback?.(progress);

      // Step 5: Cleanup temporary properties
      await this.cleanup();
      progress.stage = 'complete';
      progressCallback?.(progress);

      console.log('Database replication completed successfully!');
    } catch (error) {
      console.error('Database replication failed:', error);
      throw error;
    }
  }

  private async getTotalCounts(): Promise<{ totalNodes: number; totalRelationships: number }> {
    const sourceSession = this.sourceDriver.session();
    try {
      const nodeResult = await sourceSession.run('MATCH (n) RETURN count(n) as total');
      const relResult = await sourceSession.run('MATCH ()-[r]->() RETURN count(r) as total');
      
      return {
        totalNodes: nodeResult.records[0].get('total').toNumber(),
        totalRelationships: relResult.records[0].get('total').toNumber()
      };
    } finally {
      await sourceSession.close();
    }
  }

  private async clearTargetDatabase(): Promise<void> {
    console.log('Clearing target database...');
    const session = this.targetDriver.session();
    
    try {
      // Delete all nodes and relationships
      await session.run('MATCH (n) DETACH DELETE n');
      console.log('All nodes and relationships deleted');

      // Drop all constraints
      const constraints = await session.run('SHOW CONSTRAINTS');
      for (const constraint of constraints.records) {
        const constraintName = constraint.get('name');
        try {
          await session.run(`DROP CONSTRAINT ${constraintName}`);
          console.log(`Dropped constraint: ${constraintName}`);
        } catch (error) {
          console.warn(`Failed to drop constraint ${constraintName}:`, error);
        }
      }

      // Drop all indexes (except constraint-backed ones)
      const indexes = await session.run("SHOW INDEXES WHERE type <> 'CONSTRAINT'");
      for (const index of indexes.records) {
        const indexName = index.get('name');
        try {
          await session.run(`DROP INDEX ${indexName}`);
          console.log(`Dropped index: ${indexName}`);
        } catch (error) {
          console.warn(`Failed to drop index ${indexName}:`, error);
        }
      }
    } finally {
      await session.close();
    }
  }

  private async copySchema(): Promise<void> {
    console.log('Copying database schema...');
    
    const sourceSession = this.sourceDriver.session();
    const targetSession = this.targetDriver.session();
    
    try {
      // Copy constraints
      const constraints = await sourceSession.run('SHOW CONSTRAINTS');
      
      for (const constraint of constraints.records) {
        const name = constraint.get('name');
        const type = constraint.get('type');
        const entityType = constraint.get('entityType');
        const labelsOrTypes = constraint.get('labelsOrTypes') as string[];
        const properties = constraint.get('properties') as string[];
        
        try {
          let createStatement = '';
          
          if (type === 'UNIQUENESS') {
            if (entityType === 'NODE') {
              const label = labelsOrTypes[0];
              const propList = properties.map(p => `n.${p}`).join(', ');
              createStatement = `CREATE CONSTRAINT ${name} FOR (n:${label}) REQUIRE (${propList}) IS UNIQUE`;
            } else if (entityType === 'RELATIONSHIP') {
              const relType = labelsOrTypes[0];
              const propList = properties.map(p => `r.${p}`).join(', ');
              createStatement = `CREATE CONSTRAINT ${name} FOR ()-[r:${relType}]-() REQUIRE (${propList}) IS UNIQUE`;
            }
          } else if (type === 'NODE_PROPERTY_EXISTENCE' || type === 'EXISTENCE') {
            const label = labelsOrTypes[0];
            const property = properties[0];
            createStatement = `CREATE CONSTRAINT ${name} FOR (n:${label}) REQUIRE n.${property} IS NOT NULL`;
          } else if (type === 'RELATIONSHIP_PROPERTY_EXISTENCE') {
            const relType = labelsOrTypes[0];
            const property = properties[0];
            createStatement = `CREATE CONSTRAINT ${name} FOR ()-[r:${relType}]-() REQUIRE r.${property} IS NOT NULL`;
          } else if (type === 'NODE_KEY') {
            const label = labelsOrTypes[0];
            const propList = properties.map(p => `n.${p}`).join(', ');
            createStatement = `CREATE CONSTRAINT ${name} FOR (n:${label}) REQUIRE (${propList}) IS NODE KEY`;
          }
          
          if (createStatement) {
            await targetSession.run(createStatement);
            console.log(`Created constraint: ${name} (${type})`);
          } else {
            console.warn(`Skipped unsupported constraint type: ${type} for ${name}`);
          }
        } catch (error: any) {
          // Skip if constraint already exists or other non-critical errors
          if (error.code === 'Neo.ClientError.Schema.ConstraintAlreadyExists' || 
              error.code === 'Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists') {
            console.log(`Constraint ${name} already exists, skipping`);
          } else {
            console.warn(`Failed to create constraint ${name}:`, error.message);
          }
        }
      }

      // Copy indexes
      const indexes = await sourceSession.run("SHOW INDEXES WHERE type <> 'CONSTRAINT'");
      
      for (const index of indexes.records) {
        const name = index.get('name');
        const type = index.get('type');
        const labelsOrTypes = index.get('labelsOrTypes') as string[];
        const properties = index.get('properties') as string[];
        
        try {
          let createStatement = '';
          
          if (type === 'BTREE') {
            if (labelsOrTypes.length > 0 && labelsOrTypes[0]) {
              // Node index
              const label = labelsOrTypes[0];
              const propList = properties.join(', ');
              createStatement = `CREATE INDEX ${name} FOR (n:${label}) ON (${properties.map(p => `n.${p}`).join(', ')})`;
            }
          } else if (type === 'TEXT') {
            const label = labelsOrTypes[0];
            const property = properties[0];
            createStatement = `CREATE TEXT INDEX ${name} FOR (n:${label}) ON (n.${property})`;
          } else if (type === 'FULLTEXT') {
            const labelList = labelsOrTypes.join(', ');
            const propList = properties.map(p => `'${p}'`).join(', ');
            createStatement = `CREATE FULLTEXT INDEX ${name} FOR (n:${labelsOrTypes.join('|')}) ON EACH [${propList}]`;
          }
          
          if (createStatement) {
            await targetSession.run(createStatement);
            console.log(`Created index: ${name} (${type})`);
          } else {
            console.warn(`Skipped unsupported index type: ${type} for ${name}`);
          }
        } catch (error: any) {
          // Skip if index already exists or other non-critical errors
          if (error.code === 'Neo.ClientError.Schema.IndexAlreadyExists' || 
              error.code === 'Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists') {
            console.log(`Index ${name} already exists, skipping`);
          } else {
            console.warn(`Failed to create index ${name}:`, error.message);
          }
        }
      }
    } finally {
      await sourceSession.close();
      await targetSession.close();
    }
  }

  private async copyNodes(progressCallback?: (processed: number) => void): Promise<void> {
    console.log('Copying nodes...');
    const sourceSession = this.sourceDriver.session();
    
    try {
      const totalResult = await sourceSession.run('MATCH (n) RETURN count(n) as total');
      const totalNodes = totalResult.records[0].get('total').toNumber();
      
      let processed = 0;
      
      while (processed < totalNodes) {
        const nodesResult = await sourceSession.run(`
          MATCH (n) 
          RETURN id(n) as nodeId, labels(n) as labels, properties(n) as properties
          SKIP $skip LIMIT $limit
        `, { 
          skip: neo4j.int(processed), 
          limit: neo4j.int(this.batchSize) 
        });

        const nodesData: NodeData[] = nodesResult.records.map(record => ({
          nodeId: record.get('nodeId').toNumber(),
          labels: record.get('labels') as string[],
          properties: record.get('properties') as Record<string, any>
        }));

        if (nodesData.length > 0) {
          await this.createNodesInTarget(nodesData);
        }

        processed += nodesData.length;
        progressCallback?.(processed);
        console.log(`Processed ${processed}/${totalNodes} nodes`);
      }
    } finally {
      await sourceSession.close();
    }
  }

  private async createNodesInTarget(nodesData: NodeData[]): Promise<void> {
    const targetSession = this.targetDriver.session();
    
    try {
      // Group nodes by label combination and create separately
      const nodesByLabels = new Map<string, NodeData[]>();
      
      for (const node of nodesData) {
        const labelKey = node.labels.sort().join(',');
        if (!nodesByLabels.has(labelKey)) {
          nodesByLabels.set(labelKey, []);
        }
        nodesByLabels.get(labelKey)!.push(node);
      }

      for (const [labelKey, nodes] of nodesByLabels) {
        const labels = labelKey ? labelKey.split(',') : [];
        const labelClause = labels.length > 0 ? `:${labels.join(':')}` : '';
        
        await targetSession.run(`
          UNWIND $nodes as node
          CREATE (n${labelClause})
          SET n = node.properties, n.source_id = node.nodeId
        `, { nodes });
      }
    } finally {
      await targetSession.close();
    }
  }

  private async copyRelationships(progressCallback?: (processed: number) => void): Promise<void> {
    console.log('Copying relationships...');
    const sourceSession = this.sourceDriver.session();
    
    try {
      const totalResult = await sourceSession.run('MATCH ()-[r]->() RETURN count(r) as total');
      const totalRelationships = totalResult.records[0].get('total').toNumber();
      
      let processed = 0;
      
      while (processed < totalRelationships) {
        const relsResult = await sourceSession.run(`
          MATCH (a)-[r]->(b)
          RETURN id(a) as startId, id(b) as endId, 
                 type(r) as type, properties(r) as properties
          SKIP $skip LIMIT $limit
        `, { 
          skip: neo4j.int(processed), 
          limit: neo4j.int(this.batchSize / 2) // Smaller batches for relationships
        });

        const relsData: RelationshipData[] = relsResult.records.map(record => ({
          startId: record.get('startId').toNumber(),
          endId: record.get('endId').toNumber(),
          type: record.get('type') as string,
          properties: record.get('properties') as Record<string, any>
        }));

        if (relsData.length > 0) {
          await this.createRelationshipsInTarget(relsData);
        }

        processed += relsData.length;
        progressCallback?.(processed);
        console.log(`Processed ${processed}/${totalRelationships} relationships`);
      }
    } finally {
      await sourceSession.close();
    }
  }

  private async createRelationshipsInTarget(relsData: RelationshipData[]): Promise<void> {
    const targetSession = this.targetDriver.session();
    
    try {
      // Group by relationship type and create separately
      const relsByType = new Map<string, RelationshipData[]>();
      
      for (const rel of relsData) {
        if (!relsByType.has(rel.type)) {
          relsByType.set(rel.type, []);
        }
        relsByType.get(rel.type)!.push(rel);
      }

      for (const [relType, rels] of relsByType) {
        // Use CALL subquery for dynamic relationship type creation
        await targetSession.run(`
          UNWIND $rels as rel
          MATCH (a {source_id: rel.startId}), (b {source_id: rel.endId})
          CALL {
            WITH a, b, rel
            CREATE (a)-[r:\`${relType}\`]->(b)
            SET r = rel.properties
            RETURN r
          }
          RETURN count(*)
        `, { rels });
      }
    } finally {
      await targetSession.close();
    }
  }

  private async cleanup(): Promise<void> {
    console.log('Cleaning up temporary properties...');
    const targetSession = this.targetDriver.session();
    
    try {
      await targetSession.run('MATCH (n) REMOVE n.source_id');
      console.log('Cleanup completed');
    } finally {
      await targetSession.close();
    }
  }

  async testConnections(): Promise<{ source: boolean; target: boolean }> {
    const results = { source: false, target: false };
    
    try {
      const sourceSession = this.sourceDriver.session();
      await sourceSession.run('RETURN 1');
      await sourceSession.close();
      results.source = true;
      console.log('✓ Source database connection successful');
    } catch (error) {
      console.error('✗ Source database connection failed:', error);
    }

    try {
      const targetSession = this.targetDriver.session();
      await targetSession.run('RETURN 1');
      await targetSession.close();
      results.target = true;
      console.log('✓ Target database connection successful');
    } catch (error) {
      console.error('✗ Target database connection failed:', error);
    }

    return results;
  }

  async close(): Promise<void> {
    await this.sourceDriver.close();
    await this.targetDriver.close();
    console.log('Database connections closed');
  }
}

// Usage example with environment variables
async function main() {

  try {
    // Test connections first
    console.log('🔍 Testing database connections...');
    const connections = await replicator.testConnections();
    
    if (!connections.source) {
      console.error('❌ Failed to connect to source database');
      console.log('Check your SOURCE_NEO4J_PASSWORD and ensure Neo4j is running locally');
      Deno.exit(1);
    }
    
    if (!connections.target) {
      console.error('❌ Failed to connect to target database');
      console.log('Check your TARGET_NEO4J_URI and TARGET_NEO4J_PASSWORD');
      Deno.exit(1);
    }

    console.log('✅ All connections successful, starting replication...\n');

    // Start replication with progress tracking
    await replicator.replicateDatabase((progress) => {
      const nodePercent = progress.totalNodes > 0 ? 
        Math.round((progress.processedNodes / progress.totalNodes) * 100) : 0;
      const relPercent = progress.totalRelationships > 0 ? 
        Math.round((progress.processedRelationships / progress.totalRelationships) * 100) : 0;
      
      console.log(`📊 Stage: ${progress.stage.toUpperCase()}`);
      console.log(`   Nodes: ${progress.processedNodes}/${progress.totalNodes} (${nodePercent}%)`);
      console.log(`   Relationships: ${progress.processedRelationships}/${progress.totalRelationships} (${relPercent}%)`);
      console.log('---');
    });

    console.log('🎉 Database replication completed successfully!');
  } catch (error) {
    console.error('❌ Replication failed:', error);
    Deno.exit(1);
  } finally {
    await replicator.close();
  }
}

main().catch(console.error);
