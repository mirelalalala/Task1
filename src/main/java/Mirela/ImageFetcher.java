package Mirela;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public final class ImageFetcher {

    private static final String UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

    private static final OkHttpClient HTTP = new OkHttpClient.Builder()
            .followRedirects(true)
            .followSslRedirects(true)
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build();

    public static class Payload {
        public final byte[] bytes;
        public final String contentType;
        public Payload(byte[] bytes, String contentType) {
            this.bytes = bytes;
            this.contentType = contentType;
        }
    }

    public static Payload get(String url) throws IOException {
        return get(url, "https://www.google.com/");
    }

    public static Payload get(String url, String referer) throws IOException {
        String ref = (referer != null && !referer.isEmpty()) ? referer : "https://www.google.com/";

        // warm-up homepage pentru cookies (dacă referer e un site)
        try {
            java.net.URL r = new java.net.URL(ref);
            okhttp3.HttpUrl warmUrl = new okhttp3.HttpUrl.Builder()
                    .scheme(r.getProtocol().startsWith("http") ? r.getProtocol() : "https")
                    .host(r.getHost())
                    .addPathSegment("") // "/"
                    .build();
            Request warm = new Request.Builder()
                    .url(warmUrl)
                    .get()
                    .header("User-Agent", UA)
                    .header("Accept", "text/html,*/*;q=0.8")
                    .build();
            try (Response wr = HTTP.newCall(warm).execute()) { /* ignore body */ }
        } catch (Throwable ignore) {}

        Request req = new Request.Builder()
                .url(url)
                .get()
                .header("User-Agent", UA)
                .header("Accept", "image/avif,image/webp,image/apng,image/*,*/*;q=0.8")
                .header("Accept-Language", "en-US,en;q=0.9")
                .header("Referer", ref)
                .build();

        try (Response res = HTTP.newCall(req).execute()) {
            if (!res.isSuccessful() || res.body() == null) {
                throw new IOException("HTTP " + res.code() + " for " + url);
            }
            String ct = res.header("Content-Type", "");
            return new Payload(res.body().bytes(), ct == null ? "" : ct);
        }
    }

    private ImageFetcher() {}
}
