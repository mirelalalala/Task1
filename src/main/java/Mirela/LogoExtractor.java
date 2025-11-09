package Mirela;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;

public class LogoExtractor {

    private static final String UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

    public static class Candidate {
        public final String source;
        public final String absoluteUrl;

        public Candidate(String source, String absoluteUrl) {
            this.source = source;
            this.absoluteUrl = absoluteUrl;
        }
    }

    private static org.jsoup.Connection connect(String url) {
        return Jsoup.connect(url)
                .userAgent(UA)
                .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .header("Accept-Language", "en-US,en;q=0.9")
                .header("Upgrade-Insecure-Requests", "1")
                .header("Sec-Fetch-Mode", "navigate")
                .header("Sec-Fetch-Dest", "document")
                .header("Sec-Fetch-Site", "none")
                .header("Sec-Fetch-User", "?1")
                .referrer("https://www.google.com/")
                .followRedirects(true)
                .timeout(20000);
    }

    public List<Candidate> findAll(String siteUrl) throws Exception {
        String norm = normalizeRoot(siteUrl);

        List<String> tries = new ArrayList<>();
        tries.add(norm);
        if (!norm.contains("://www.")) {
            tries.add(norm.replace("://", "://www."));
        }
        tries.add(norm.replace("https://", "http://"));
        if (!norm.contains("://www.")) {
            tries.add(norm.replace("https://", "http://www."));
        }

        Document doc = null;
        for (String t : tries) {
            try {
                Document d = connect(t).get();
                if (d != null) {
                    doc = d;
                    norm = t;
                    break;
                }
            } catch (Exception ignore) {}
        }

        if (doc == null) {
            List<Candidate> out = new ArrayList<>();
            addIfUrl(out, "apple-touch-probe", toAbsolute(norm, "/apple-touch-icon.png"));
            addIfUrl(out, "apple-touch-probe", toAbsolute(norm, "/apple-touch-icon-precomposed.png"));
            addIfUrl(out, "apple-touch-probe", toAbsolute(norm, "/apple-touch-icon-180x180.png"));
            addIfUrl(out, "apple-touch-probe", toAbsolute(norm, "/apple-touch-icon-152x152.png"));
            addIfUrl(out, "apple-touch-probe", toAbsolute(norm, "/apple-touch-icon-120x120.png"));
            addIfUrl(out, "favicon", toAbsolute(norm, "/favicon.ico"));
            return dedupeByUrl(prioritize(out));
        }

        List<Candidate> out = new ArrayList<>();

        for (Element el : doc.select(
                "link[rel~=(?i)icon], " +
                        "link[rel~=(?i)shortcut][rel~=(?i)icon], " +
                        "link[rel~=(?i)mask-icon], " +
                        "link[rel~=(?i)apple-touch-icon]")) {
            addIfUrl(out, "rel-icon", absUrl(doc, el.attr("href")));
        }

        for (Element el : doc.select(
                "meta[property=og:image], " +
                        "meta[name=twitter:image], " +
                        "meta[property=og:image:secure_url]")) {
            addIfUrl(out, "meta", absUrl(doc, el.attr("content")));
        }

        String q = String.join(",",
                "img[alt*=logo i]",
                "[class*=logo i] img",
                "img[class*=logo i]",
                "img[id*=logo i]",
                "[class*=brand i] img",
                "img[id*=brand i]",
                "header img",
                "nav img",
                ".navbar-brand img",
                ".site-branding img");
        for (Element el : doc.select(q)) {
            addIfUrl(out, "header-img", absUrl(doc, el.attr("src")));
        }

        for (Element el : doc.select("link[rel=manifest]")) {
            String murl = absUrl(doc, el.attr("href"));
            if (murl == null || murl.isEmpty()) continue;
            try {
                byte[] jsonData = ImageFetcher.get(murl).bytes;
                ObjectMapper om = new ObjectMapper();
                JsonNode root = om.readTree(jsonData);
                if (root != null && root.has("icons") && root.get("icons").isArray()) {
                    for (JsonNode ic : root.get("icons")) {
                        if (ic.has("src")) {
                            String src = ic.get("src").asText();
                            addIfUrl(out, "manifest", toAbsolute(murl, src));
                        }
                    }
                }
            } catch (Exception ignore) {}
        }

        addIfUrl(out, "apple-touch-probe", toAbsolute(norm, "/apple-touch-icon.png"));
        addIfUrl(out, "apple-touch-probe", toAbsolute(norm, "/apple-touch-icon-precomposed.png"));
        addIfUrl(out, "apple-touch-probe", toAbsolute(norm, "/apple-touch-icon-180x180.png"));
        addIfUrl(out, "apple-touch-probe", toAbsolute(norm, "/apple-touch-icon-152x152.png"));
        addIfUrl(out, "apple-touch-probe", toAbsolute(norm, "/apple-touch-icon-120x120.png"));
        addIfUrl(out, "favicon", toAbsolute(norm, "/favicon.ico"));

        return dedupeByUrl(prioritize(out));
    }

    private static void addIfUrl(List<Candidate> list, String src, String url) {
        if (url != null && !url.isEmpty()) {
            list.add(new Candidate(src, url));
        }
    }

    private static String absUrl(Document doc, String href) {
        try {
            if (href == null || href.trim().isEmpty()) return null;
            return new URL(new URL(doc.baseUri()), href).toString();
        } catch (Exception e) {
            return null;
        }
    }

    private static String toAbsolute(String base, String rel) {
        try {
            if (rel == null || rel.trim().isEmpty()) return null;
            return new URL(new URL(base), rel).toString();
        } catch (Exception e) {
            return null;
        }
    }

    private static String normalizeRoot(String u) throws Exception {
        String in = u.trim();
        if (!in.startsWith("http")) in = "https://" + in;
        URL url = new URL(in);
        String host = url.getHost();
        if (!host.startsWith("www.")) host = "www." + host;
        int port = url.getPort();
        return new URL(url.getProtocol(), host, port, "/").toString();
    }

    private static List<Candidate> dedupeByUrl(List<Candidate> in) {
        LinkedHashMap<String, Candidate> map = new LinkedHashMap<>();
        for (Candidate c : in) {
            if (c == null || c.absoluteUrl == null) continue;
            String key = stripQuery(c.absoluteUrl);
            if (!map.containsKey(key)) {
                map.put(key, new Candidate(c.source, key));
            }
        }
        return new ArrayList<>(map.values());
    }

    private static String stripQuery(String u) {
        try {
            URL url = new URL(u);
            return new URL(url.getProtocol(), url.getHost(), url.getPort(), url.getPath()).toString();
        } catch (Exception e) {
            return u;
        }
    }

    private static List<Candidate> prioritize(List<Candidate> in) {
        List<Candidate> out = new ArrayList<>(in);
        out.sort((a, b) -> score(b) - score(a));
        return out;
    }

    private static int score(Candidate c) {
        return score(c.absoluteUrl);
    }

    private static int score(String u) {
        if (u == null) return 0;
        String s = u.toLowerCase(Locale.ROOT);
        int sc = 0;
        if (s.endsWith(".svg")) sc += 50;
        else if (s.endsWith(".png")) sc += 45;
        else if (s.endsWith(".jpg") || s.endsWith(".jpeg")) sc += 35;
        else if (s.endsWith(".webp")) sc += 32;
        else if (s.endsWith(".gif")) sc += 10;
        else if (s.endsWith(".bmp")) sc += 5;
        else if (s.endsWith(".ico")) sc -= 50;
        if (s.contains("logo")) sc += 40;
        if (s.contains("brand")) sc += 15;
        if (s.contains("512") || s.contains("512x512")) sc += 25;
        else if (s.contains("256") || s.contains("256x256")) sc += 20;
        else if (s.contains("192") || s.contains("192x192")) sc += 18;
        else if (s.contains("180x180")) sc += 18;
        else if (s.contains("152x152")) sc += 16;
        else if (s.contains("120x120")) sc += 14;
        if (s.contains("favicon")) sc -= 40;
        return sc;
    }
}
