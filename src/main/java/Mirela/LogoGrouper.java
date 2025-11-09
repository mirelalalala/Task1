package Mirela;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.*;

public class LogoGrouper {
    
    static class LogoItem {
        String domain;
        String logoUrl;
        long hash;
        
        LogoItem(String d, String u, long h) {
            domain = d;
            logoUrl = u;
            hash = h;
        }
    }
    
    public static void main(String[] args) throws Exception {
        String parquetPath = args.length > 0 ? args[0] : "data/logos.snappy.parquet";
        String resultsCsv = args.length > 1 ? args[1] : "results.csv";
        String outputFile = args.length > 2 ? args[2] : "logo_groups.csv";
        
        System.out.println("=== Logo Grouping by Similarity ===");
        System.out.println("Input: " + parquetPath);
        System.out.println("Output: " + outputFile);
        System.out.println();
        
        int totalDomainsInParquet = getTotalDomainsInParquet(parquetPath);
        System.out.println("Total domains in parquet: " + totalDomainsInParquet);
        System.out.println();
         

        System.out.println("Step 1: Extracting logos from parquet file...");
        Map<String, String> domainToLogo = extractLogos(parquetPath, resultsCsv);
        int extractedCount = domainToLogo.size();
        System.out.println("Extracted " + extractedCount + " logos");
                
        
        double extractionRate = totalDomainsInParquet > 0 ? (100.0 * extractedCount / totalDomainsInParquet) : 0;
        System.out.println();
        System.out.println("EXTRACTION SUCCESS RATE: " + String.format("%.2f%%", extractionRate) +
                         " (" + extractedCount + "/" + totalDomainsInParquet + ")");
        if (extractionRate >= 97.0) {
            System.out.println(" SUCCESS: Extraction rate is >= 97% (REQUIREMENT MET)");
        }
        System.out.println();
        
        System.out.println("Step 2: Hashing logos...");
        List<LogoItem> items = hashLogos(domainToLogo);
        System.out.println("Hashed " + items.size() + " logos");
        
        System.out.println("\nStep 3: Grouping similar logos...");
        Map<Integer, List<LogoItem>> groups = groupLogos(items);
        System.out.println("Created " + groups.size() + " groups");
        
        System.out.println("\nStep 4: Writing results...");
        writeGroups(groups, outputFile);
        
        long similarGroupsCount = groups.values().stream()
            .filter(g -> g.size() >= 2)
            .count();
        long uniqueLogosCount = groups.values().stream()
            .filter(g -> g.size() == 1)
            .count();
        
        System.out.println("\n Done! Results written to: " + outputFile);
        System.out.println();
        System.out.println("FINAL STATISTICS:");
        System.out.println("  Total domains: " + totalDomainsInParquet);
        System.out.println("  Successfully extracted: " + extractedCount);
        System.out.println("  Extraction rate: " + String.format("%.2f%%", extractionRate));
        System.out.println("  Successfully hashed: " + items.size());
        System.out.println("  Similar logo groups: " + similarGroupsCount);
        System.out.println("  Unique logos: " + uniqueLogosCount);
        System.out.println("  Output file: " + outputFile);
    }
    
    private static int getTotalDomainsInParquet(String parquetPath) {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            try (Connection con = DriverManager.getConnection("jdbc:duckdb:");
                 Statement st = con.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT COUNT(DISTINCT domain) FROM read_parquet('" + 
                         parquetPath.replace("\\", "/") + "') " +
                         "WHERE domain IS NOT NULL AND domain <> ''")) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (Exception e) {
            System.err.println("Warning: Could not read parquet file: " + e.getMessage());
            return 0;
        }
        return 0;
    }
    
    private static Map<String, String> loadFromResults(String resultsCsv) throws Exception {
        Map<String, String> domainToLogo = new HashMap<>();
        
        try (java.io.BufferedReader r = Files.newBufferedReader(Path.of(resultsCsv))) {
            String line;
            boolean skipHeader = true;
            while ((line = r.readLine()) != null) {
                if (skipHeader) {
                    skipHeader = false;
                    continue;
                }
                if (line.isBlank()) continue;
                
                List<String> cols = parseCsv(line);
                if (cols.size() >= 4) {
                    String status = unquote(cols.get(3));
                    if (status != null && status.equalsIgnoreCase("OK")) {
                        String domain = unquote(cols.get(0));
                        String logoUrl = unquote(cols.get(2));
                        if (domain != null && logoUrl != null && !logoUrl.isEmpty()) {
                            domainToLogo.put(domain, logoUrl);
                        }
                    }
                }
            }
        }
        
        return domainToLogo;
    }

private static Map<String, String> extractLogos(String parquetPath, String resultsCsv) throws Exception {
    Map<String, String> domainToLogo = new HashMap<>();
    LogoExtractor extractor = new LogoExtractor();
    int processed = 0;
    int success = 0;
    
    Class.forName("org.duckdb.DuckDBDriver");
    
    try (BufferedWriter csvWriter = Files.newBufferedWriter(Path.of(resultsCsv),
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
         Connection con = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = con.createStatement();
         ResultSet rs = st.executeQuery(
                 "SELECT DISTINCT domain FROM read_parquet('" + 
                 parquetPath.replace("\\", "/") + "') " +
                 "WHERE domain IS NOT NULL AND domain <> ''")) {
        
        csvWriter.write("domain,home_url,logo_url,status,error\n");
        
        while (rs.next()) {
            String domain = rs.getString(1).trim();
            processed++;
            
            if (processed % 100 == 0) {
                System.out.println("  Processed: " + processed + ", Success: " + success);
                csvWriter.flush(); // Flush periodically
            }
            
            String homeUrl = null;
            String logoUrl = null;
            String status = "OK";
            String error = "";
            
            try {
                List<String> urlVariations = new ArrayList<>();
                String baseDomain = domain;
                if (!baseDomain.startsWith("http://") && !baseDomain.startsWith("https://")) {
                    urlVariations.add("https://" + baseDomain);
                    urlVariations.add("https://www." + baseDomain);
                    urlVariations.add("http://" + baseDomain);
                    urlVariations.add("http://www." + baseDomain);
                } else {
                    urlVariations.add(baseDomain);
                    // Add www version
                    if (baseDomain.contains("://www.")) {
                        urlVariations.add(baseDomain.replace("://www.", "://"));
                    } else {
                        urlVariations.add(baseDomain.replace("://", "://www."));
                    }
                }
                
                List<LogoExtractor.Candidate> logos = new ArrayList<>();
                Exception lastException = null;
                
                for (String siteUrl : urlVariations) {
                    try {
                        List<LogoExtractor.Candidate> candidates = extractor.findAll(siteUrl);
                        if (!candidates.isEmpty()) {
                            logos = candidates;
                            homeUrl = siteUrl;
                            break;
                        }
                    } catch (Exception e) {
                        lastException = e;
                    }
                }
                
                if (logos.isEmpty()) {
                    status = "NO_LOGO";
                } else {
                    String referer = buildReferer(homeUrl != null ? homeUrl : domain);
                    boolean found = false;
                    
                    for (LogoExtractor.Candidate c : logos) {
                        try {
                            ImageFetcher.Payload payload = fetchWithRetry(c.absoluteUrl, referer, 2);                            String ct = payload.contentType == null ? "" : payload.contentType.toLowerCase();
                            java.awt.image.BufferedImage img = ImageDecoder.decode(
                                payload.bytes, 
                                c.absoluteUrl.toLowerCase(), 
                                ct
                            );
                            
                            if (img != null && img.getWidth() >= 16 && img.getHeight() >= 16) {
                                homeUrl = homeUrl != null ? homeUrl : domain;
                                logoUrl = c.absoluteUrl;
                                status = "OK";
                                domainToLogo.put(domain, c.absoluteUrl);
                                success++;
                                found = true;
                                break;
                            }
                        } catch (Exception e) {
                        }
                    }
                    
                    if (!found) {
                        status = "UNREADABLE";
                    }
                }
            } catch (Exception e) {
                status = "ERROR";
                error = e.getClass().getSimpleName();
                if (e.getMessage() != null && !e.getMessage().isEmpty()) {
                    error += ": " + e.getMessage().substring(0, Math.min(50, e.getMessage().length()));
                }
            }
           
            
            csvWriter.write(csv(domain) + "," + csv(homeUrl) + "," + csv(logoUrl) + "," + csv(status) + "," + csv(error) + "\n");
            
            Thread.sleep(50);
        }
        
        csvWriter.flush();
    }
    
    System.out.println("  Final: " + processed + " processed, " + success + " successful");
    System.out.println("  Results saved to: " + resultsCsv);
    return domainToLogo;
}
private static ImageFetcher.Payload fetchWithRetry(String url, String referer, int maxRetries) throws IOException {
    Exception lastEx = null;
    for (int i = 0; i < maxRetries; i++) {
        try {
            return ImageFetcher.get(url, referer);
        } catch (Exception e) {
            lastEx = e;
            if (i < maxRetries - 1) {
                try {
                    Thread.sleep(100 * (i + 1));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted", ie);
                }
            }
        }
    }
    throw new IOException("Failed after " + maxRetries + " retries: " + 
        (lastEx != null ? lastEx.getMessage() : "unknown"), lastEx);
}
     private static List<LogoItem> hashLogos(Map<String, String> domainToLogo) {
        List<LogoItem> items = new ArrayList<>();
        int count = 0;
        
        for (Map.Entry<String, String> entry : domainToLogo.entrySet()) {
            count++;
            if (count % 100 == 0) {
                System.out.println("  Hashed: " + count + "/" + domainToLogo.size());
            }
            
            long hash = ImageHasher.dHash(entry.getValue());
            if (hash != -1) {
                items.add(new LogoItem(entry.getKey(), entry.getValue(), hash));
            }
        }
        
        return items;
    }

    private static Map<Integer, List<LogoItem>> groupLogos(List<LogoItem> items) {
        final int THRESHOLD = 8;
        
        int n = items.size();
        int[] parent = new int[n];
        for (int i = 0; i < n; i++) parent[i] = i;
        
        Map<Long, List<Integer>> buckets = new HashMap<>();
        long mask = 0xFFF;
        for (int i = 0; i < n; i++) {
            long key = (items.get(i).hash >>> 52) & mask;
            buckets.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
        }
        
        System.out.println("  Comparing logos for similarity (threshold: " + THRESHOLD + " bits difference)...");
        int comparisons = 0;
        int unions = 0;

        for (List<Integer> bucket : buckets.values()) {
            for (int i = 0; i < bucket.size(); i++) {
                for (int j = i + 1; j < bucket.size(); j++) {
                    int idx1 = bucket.get(i);
                    int idx2 = bucket.get(j);
                    long h1 = items.get(idx1).hash;
                    long h2 = items.get(idx2).hash;
                    int dist = Long.bitCount(h1 ^ h2);
                    comparisons++;
                    
                    if (dist <= THRESHOLD) {
                        if (find(parent, idx1) != find(parent, idx2)) {
                            union(parent, idx1, idx2);
                            unions++;
                        }
                    }
                }
            }
        }
        
        List<Long> bucketKeys = new ArrayList<>(buckets.keySet());
        for (int i = 0; i < bucketKeys.size(); i++) {
            for (int j = i + 1; j < bucketKeys.size(); j++) {
                long key1 = bucketKeys.get(i);
                long key2 = bucketKeys.get(j);
                if (Long.bitCount(key1 ^ key2) <= 2) {
                    List<Integer> list1 = buckets.get(key1);
                    List<Integer> list2 = buckets.get(key2);
                    for (int idx1 : list1) {
                        for (int idx2 : list2) {
                            comparisons++;
                            long h1 = items.get(idx1).hash;
                            long h2 = items.get(idx2).hash;
                            int dist = Long.bitCount(h1 ^ h2);
                            if (dist <= THRESHOLD) {
                                if (find(parent, idx1) != find(parent, idx2)) {
                                    union(parent, idx1, idx2);
                                    unions++;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        System.out.println("  Made " + comparisons + " comparisons, created " + unions + " connections");
        Map<Integer, List<LogoItem>> groups = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            int root = find(parent, i);
            groups.computeIfAbsent(root, k -> new ArrayList<>()).add(items.get(i));
        }
        
        return groups;
    }
    

    private static void writeGroups(Map<Integer, List<LogoItem>> groups, String outputFile) throws Exception {
        List<Map.Entry<Integer, List<LogoItem>>> sorted = new ArrayList<>(groups.entrySet());
        sorted.sort((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()));
        
        try (BufferedWriter w = Files.newBufferedWriter(Path.of(outputFile),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            w.write("group_id,count,websites\n");
            
            int gid = 1;
            int totalGrouped = 0;
            for (Map.Entry<Integer, List<LogoItem>> entry : sorted) {
                List<LogoItem> group = entry.getValue();
                
                if (group.size() >= 2) {
                    group.sort(Comparator.comparing(x -> x.domain.toLowerCase()));
                    
                    List<String> domains = new ArrayList<>();
                    for (LogoItem item : group) {
                        domains.add(item.domain);
                    }
                    
                    w.write(gid + "," + group.size() + "," + csv(String.join(" | ", domains)) + "\n");
                    totalGrouped += group.size();
                    gid++;
                }
            }
            
            System.out.println("  Wrote " + (gid - 1) + " groups with " + totalGrouped + " websites");
            System.out.println("  (Groups with only 1 website are not included - they have unique logos)");
        }
    }
    
    private static int find(int[] p, int x) {

        return p[x] == x ? x : (p[x] = find(p, p[x]));
    }
    
    private static void union(int[] p, int a, int b) {
        a = find(p, a);
        b = find(p, b);
        if (a != b) p[b] = a;
    }
    
    private static String buildReferer(String domain) {
        try {
            String d = domain;
            if (!d.startsWith("http")) d = "https://" + d;
            java.net.URI uri = java.net.URI.create(d);
            String scheme = uri.getScheme();
            String host = uri.getHost();
            if (scheme == null) scheme = "https";
            if (host == null) return "https://www.google.com/";
            return scheme + "://" + host + "/";
        } catch (Exception e) {
            return "https://www.google.com/";
        }
    }
    
    private static String csv(String s) {
        if (s == null) return "\"\"";
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }
    
    private static List<String> parseCsv(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQ = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (inQ && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cur.append('"');
                    i++;
                } else {
                    inQ = !inQ;
                }
            } else if (ch == ',' && !inQ) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        out.add(cur.toString());
        return out;
    }
    
    private static String unquote(String s) {
        if (s == null) return null;
        s = s.trim();
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1).replace("\"\"", "\"");
        }
        return s;
    }
}

