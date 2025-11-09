package Mirela;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import org.apache.commons.imaging.Imaging;

public class ImageDecoder {
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

    public static BufferedImage decode(byte[] imgBytes, String urlLower, String contentTypeLower) {
        try (ByteArrayInputStream bin = new ByteArrayInputStream(imgBytes)) {
            BufferedImage img = ImageIO.read(bin);
            if (img != null && img.getWidth() > 0 && img.getHeight() > 0) return img;
        } catch (Throwable ignore) {}
        
        if (looksIco(urlLower, contentTypeLower)) {
            try {
                BufferedImage img = Imaging.getBufferedImage(imgBytes);
                if (img != null && img.getWidth() > 0 && img.getHeight() > 0) return img;
            } catch (Throwable ignore) {}
        }
        
        try {
            BufferedImage img = Imaging.getBufferedImage(imgBytes);
            if (img != null && img.getWidth() > 0 && img.getHeight() > 0) return img;
        } catch (Throwable ignore) {}
        
        try {
            BufferedImage img = rasterizeSvg(imgBytes, 512);
            if (img != null && img.getWidth() > 0 && img.getHeight() > 0) return img;
        } catch (Throwable ignore) {}
        
        try {
            BufferedImage img = rasterizeSvg(imgBytes, 256);
            if (img != null && img.getWidth() > 0 && img.getHeight() > 0) return img;
        } catch (Throwable ignore) {}
        
        try {
            BufferedImage img = rasterizeSvg(imgBytes, 1024);
            if (img != null && img.getWidth() > 0 && img.getHeight() > 0) return img;
        } catch (Throwable ignore) {}
        
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
}

