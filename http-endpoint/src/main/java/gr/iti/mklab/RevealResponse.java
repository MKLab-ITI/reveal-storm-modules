package gr.iti.mklab;

/**
 * @author kandreadou
 */
public class RevealResponse {

    protected boolean success = true;

    protected String message;

    public RevealResponse(boolean success, String message) {
        this.success = success;
        this.message = message;

    }
}
