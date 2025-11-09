package Mirela;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.util.Locale;
import org.apache.commons.imaging.Imaging;

public class ImageHasher {
    
    private static class BITranscoder extends org.apache.batik.transcoder.image.ImageTranscoder {
        private BufferedImage image;
        @Override 
        public BufferedImage createImage(int w, int h) {
            return new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
        }
        @Override 
        public void writeImage(BufferedImage img, org.apache.batik.transcoder.TranscoderOutput out) {
            this.image = img;
        }
        public BufferedImage getImage() { return image; }
    }

    public static BufferedImage loadImage(String imageUrl) {
        return loadImage(imageUrl, guessReferer(imageUrl));
    }
    
    public static BufferedImage loadImage(String imageUrl, String referer) {
        try {
            ImageFetcher.Payload payload = ImageFetcher.get(imageUrl, referer);
            String urlLower = imageUrl.toLowerCase(Locale.ROOT);
            String ct = payload.contentType == null ? "" : payload.contentType.toLowerCase(Locale.ROOT);
            
            BufferedImage img = tryDecode(payload.bytes, urlLower, ct);
            if (img == null || img.getWidth() * img.getHeight() < 16) {
                return null;
            }
            return img;
        } catch (Exception e) {
            return null;
        }
    }

    private static BufferedImage tryDecode(byte[] imgBytes, String urlLower, String contentTypeLower) throws Exception {
        try (ByteArrayInputStream bin = new ByteArrayInputStream(imgBytes)) {
            BufferedImage img = ImageIO.read(bin);
            if (img != null) return img;
        } catch (Throwable ignore) {}
        
        if (looksIco(urlLower, contentTypeLower)) {
            try {
                return Imaging.getBufferedImage(imgBytes);
            } catch (Throwable ignore) {}
        }
        
        try {
            return Imaging.getBufferedImage(imgBytes);
        } catch (Throwable ignore) {}
        
        if ((urlLower != null && urlLower.endsWith(".svg")) ||
            (contentTypeLower != null && contentTypeLower.contains("image/svg"))) {
            try {
                return rasterizeSvg(imgBytes, 512);
            } catch (Throwable ignore) {}
        }
        
        return null;
    }
    
    private static BufferedImage rasterizeSvg(byte[] svgBytes, int targetWidth) throws Exception {
        BITranscoder t = new BITranscoder();
        if (targetWidth > 0) {
            t.addTranscodingHint(org.apache.batik.transcoder.SVGAbstractTranscoder.KEY_WIDTH, Float.valueOf(targetWidth));
        }
        org.apache.batik.transcoder.TranscoderInput in =
                new org.apache.batik.transcoder.TranscoderInput(new ByteArrayInputStream(svgBytes));
        t.transcode(in, null);
        return t.getImage();
    }
    
    private static boolean looksIco(String urlLower, String contentTypeLower) {
        if (urlLower != null && urlLower.endsWith(".ico")) return true;
        if (contentTypeLower == null) return false;
        return contentTypeLower.contains("image/x-icon") || 
               contentTypeLower.contains("image/vnd.microsoft.icon");
    }
    
    private static String guessReferer(String imageUrl) {
        try {
            java.net.URI uri = java.net.URI.create(imageUrl);
            String scheme = uri.getScheme();
            String host = uri.getHost();
            if (scheme == null) scheme = "https";
            if (host == null) return "https://example.com/";
            return (scheme + "://" + host + "/");
        } catch (Exception e) {
            return "https://example.com/";
        }
    }

    public static long dHash(String imageUrl) {
        try {
            BufferedImage image = loadImage(imageUrl);
            if (image == null) {
                return -1;
            }
            if (image.getColorModel().hasAlpha()) {
                BufferedImage opaque = new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_INT_RGB);
                Graphics2D g2 = opaque.createGraphics();
                g2.setPaint(Color.WHITE);
                g2.fillRect(0, 0, opaque.getWidth(), opaque.getHeight());
                g2.drawImage(image, 0, 0, null);
                g2.dispose();
                image = opaque;
            }
            
            BufferedImage small = new BufferedImage(9, 8, BufferedImage.TYPE_INT_RGB);
            Graphics2D g = small.createGraphics();
            g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
            g.drawImage(image, 0, 0, 9, 8, null);
            g.dispose();
            
            if (small.getWidth() < 9 || small.getHeight() < 8) {
                return -1;
            }
            long hash = 0L;
            long bit = 0;
            for (int y = 0; y < 8; y++) {
                for (int x = 0; x < 8; x++) {
                    int rgbL = small.getRGB(x, y);
                    int rgbR = small.getRGB(x + 1, y);
                    
                    int rL = (rgbL >> 16) & 0xFF, gL = (rgbL >> 8) & 0xFF, bL = rgbL & 0xFF;
                    int rR = (rgbR >> 16) & 0xFF, gR = (rgbR >> 8) & 0xFF, bR = rgbR & 0xFF;
                    
                    double yL = 0.2126 * rL + 0.7152 * gL + 0.0722 * bL;
                    double yR = 0.2126 * rR + 0.7152 * gR + 0.0722 * bR;
                    
                    if (yL > yR) {
                        hash |= (1L << bit);
                    }
                    bit++;
                }
            }
            
            return hash;
        } catch (Exception e) {
            return -1;
        }
    }
    public static int hammingDistance(long h1, long h2) {
        if (h1 == -1 || h2 == -1) {
            return Integer.MAX_VALUE;
        }
        return Long.bitCount(h1 ^ h2);
    }
}
