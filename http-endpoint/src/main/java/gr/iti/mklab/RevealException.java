package gr.iti.mklab;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * @author kandreadou
 */
@ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR, reason = "Indexing Service Exception") //500
public class RevealException extends Exception {

    public RevealException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
