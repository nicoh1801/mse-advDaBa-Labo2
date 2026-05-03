package mse.advDB;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.time.Instant;
import java.util.*;

import static org.neo4j.driver.Values.parameters;

public class Loader {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String jsonUrl = getenv("JSON_FILE_URL", "http://vmrum.isc.heia-fr.ch/files/test.jsonl");
        int maxArticles = Integer.parseInt(getenv("MAX_NODES", "10000"));
        int batchSize = Integer.parseInt(getenv("BATCH_SIZE", "1000"));

        String neo4jIp = getenv("NEO4J_IP", "localhost");
        String neo4jUser = getenv("NEO4J_USER", "neo4j");
        String neo4jPassword = getenv("NEO4J_PASSWORD", "test");

        System.out.println("JSON_URL=" + jsonUrl);
        System.out.println("MAX_ARTICLES=" + maxArticles);
        System.out.println("BATCH_SIZE=" + batchSize);
        System.out.println("NEO4J_IP=" + neo4jIp);

        Driver driver = GraphDatabase.driver(
                "bolt://" + neo4jIp + ":7687",
                AuthTokens.basic(neo4jUser, neo4jPassword)
        );

        waitForNeo4j(driver);
        System.out.println("LOAD_START=" + Instant.now());

        long start = System.currentTimeMillis();


        Session session = driver.session();

        try {
            createConstraints(session);

            int articleCount = 0;
            Set<String> uniqueAuthors = new HashSet<String>();

            List<Map<String, Object>> articleBatch = new ArrayList<Map<String, Object>>();
            List<Map<String, Object>> authorBatch = new ArrayList<Map<String, Object>>();
            List<Map<String, Object>> authoredBatch = new ArrayList<Map<String, Object>>();
            List<Map<String, Object>> citationBatch = new ArrayList<Map<String, Object>>();

            URL url = new URL(jsonUrl);
            BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()));

            String line;

            while ((line = br.readLine()) != null && articleCount < maxArticles) {
                JsonNode json = mapper.readTree(line);

                String articleId = getText(json, "id");
                String title = getText(json, "title");

                if (articleId == null || articleId.trim().isEmpty()) {
                    continue;
                }

                Map<String, Object> articleMap = new HashMap<String, Object>();
                articleMap.put("id", articleId);
                articleMap.put("title", title == null ? "" : title);
                articleBatch.add(articleMap);

                articleCount++;

                JsonNode authors = json.get("authors");
                if (authors != null && authors.isArray()) {
                    for (JsonNode author : authors) {
                        String authorId = getText(author, "id");
                        String authorName = getText(author, "name");

                        if (authorId == null || authorId.trim().isEmpty()) {
                            continue;
                        }

                        uniqueAuthors.add(authorId);

                        Map<String, Object> authorMap = new HashMap<String, Object>();
                        authorMap.put("id", authorId);
                        authorMap.put("name", authorName == null ? "" : authorName);
                        authorBatch.add(authorMap);

                        Map<String, Object> authoredMap = new HashMap<String, Object>();
                        authoredMap.put("authorId", authorId);
                        authoredMap.put("articleId", articleId);
                        authoredBatch.add(authoredMap);
                    }
                }

                JsonNode references = json.get("references");
                if (references != null && references.isArray()) {
                    for (JsonNode ref : references) {
                        if (ref == null || !ref.isTextual()) {
                            continue;
                        }

                        String citedId = ref.asText();

                        Map<String, Object> citationMap = new HashMap<String, Object>();
                        citationMap.put("articleId", articleId);
                        citationMap.put("citedId", citedId);
                        citationBatch.add(citationMap);
                    }
                }

                if (articleCount % batchSize == 0) {
                    insertBatch(session, articleBatch, authorBatch, authoredBatch, citationBatch);

                    long elapsed = (System.currentTimeMillis() - start) / 1000;

                    System.out.println(
                            "PROGRESS articles=" + articleCount +
                                    " authors=" + uniqueAuthors.size() +
                                    " totalNodes=" + (articleCount + uniqueAuthors.size()) +
                                    " elapsedSeconds=" + elapsed
                    );

                    articleBatch.clear();
                    authorBatch.clear();
                    authoredBatch.clear();
                    citationBatch.clear();
                }
            }

            br.close();

            if (!articleBatch.isEmpty()) {
                insertBatch(session, articleBatch, authorBatch, authoredBatch, citationBatch);
            }

            long duration = (System.currentTimeMillis() - start) / 1000;
            int totalNodes = articleCount + uniqueAuthors.size();

            System.out.println("LOAD_END=" + Instant.now());
            System.out.println("ARTICLES_LOADED=" + articleCount);
            System.out.println("AUTHORS_LOADED=" + uniqueAuthors.size());
            System.out.println("TOTAL_NODES=" + totalNodes);
            System.out.println("DURATION_SECONDS=" + duration);

        } finally {
            session.close();
            driver.close();
        }
    }

    private static void insertBatch(
            Session session,
            final List<Map<String, Object>> articles,
            final List<Map<String, Object>> authors,
            final List<Map<String, Object>> authored,
            final List<Map<String, Object>> citations
    ) {
        session.writeTransaction(tx -> {
            tx.run(
                    "UNWIND $articles AS row " +
                            "MERGE (a:Article {_id: row.id}) " +
                            "SET a.title = row.title",
                    parameters("articles", articles)
            );

            tx.run(
                    "UNWIND $authors AS row " +
                            "MERGE (a:Author {_id: row.id}) " +
                            "SET a.name = row.name",
                    parameters("authors", authors)
            );

            tx.run(
                    "UNWIND $authored AS row " +
                            "MATCH (author:Author {_id: row.authorId}) " +
                            "MATCH (article:Article {_id: row.articleId}) " +
                            "MERGE (author)-[:AUTHORED]->(article)",
                    parameters("authored", authored)
            );

            tx.run(
                    "UNWIND $citations AS row " +
                            "MATCH (article:Article {_id: row.articleId}) " +
                            "MERGE (cited:Article {_id: row.citedId}) " +
                            "MERGE (article)-[:CITES]->(cited)",
                    parameters("citations", citations)
            );

            return null;
        });
    }

    private static void createConstraints(Session session) {
        session.writeTransaction(tx -> {
            tx.run(
                    "CREATE CONSTRAINT article_id IF NOT EXISTS " +
                            "FOR (a:Article) " +
                            "REQUIRE a._id IS UNIQUE"
            );

            tx.run(
                    "CREATE CONSTRAINT author_id IF NOT EXISTS " +
                            "FOR (a:Author) " +
                            "REQUIRE a._id IS UNIQUE"
            );

            return null;
        });
    }

    private static void waitForNeo4j(Driver driver) throws InterruptedException {
        boolean connected = false;

        while (!connected) {
            try {
                System.out.println("Waiting for Neo4j...");
                driver.verifyConnectivity();
                connected = true;
            } catch (Exception e) {
                Thread.sleep(5000);
            }
        }

        System.out.println("Neo4j is ready.");
    }

    private static String getText(JsonNode node, String field) {
        JsonNode value = node.get(field);

        if (value == null || value.isNull()) {
            return null;
        }

        return value.asText();
    }

    private static String getenv(String key, String defaultValue) {
        String value = System.getenv(key);

        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }

        return value;
    }
}