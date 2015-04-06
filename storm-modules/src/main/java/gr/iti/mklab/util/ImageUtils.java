package gr.iti.mklab.util;

import gr.iti.mklab.visual.utilities.ImageIOGreyScale;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Image helper methods
 *
 * @author kandreadou
 */
public class ImageUtils {

    private final static int CONNECT_TIMEOUT = 3000;
    private final static int READ_TIMEOUT = 2000;

    public static BufferedImage downloadImage(String imageUrl) throws Exception {
        BufferedImage image = null;
        InputStream in = null;
        try { // first try reading with the default class
            URL url = new URL(imageUrl);
            HttpURLConnection conn = null;
            boolean success = false;
            try {
                conn = (HttpURLConnection) url.openConnection();
                conn.setInstanceFollowRedirects(true);
                conn.setConnectTimeout(CONNECT_TIMEOUT); // TO DO: add retries when connections times out
                conn.setReadTimeout(READ_TIMEOUT);
                conn.connect();
                success = true;
            } catch (Exception e) {
                System.out.println("Connection related exception at url: " + imageUrl);
            } finally {
                if (!success) {
                    conn.disconnect();
                }
            }
            success = false;
            try {
                in = conn.getInputStream();
                success = true;
            } catch (Exception e) {
                System.out.println("Exception when getting the input stream from the connection at url: "
                        + imageUrl);
            } finally {
                if (!success) {
                    in.close();
                }
            }
            image = ImageIO.read(in);
        } catch (IllegalArgumentException e) {
            // this exception is probably thrown because of a greyscale jpeg image
            System.out.println("Exception: " + e.getMessage() + " | Image: " + imageUrl);
            image = ImageIOGreyScale.read(in); // retry with the modified class
        } catch (MalformedURLException e) {
            System.out.println("Malformed url exception. Url: " + imageUrl);
        }
        return image;
    }
}
