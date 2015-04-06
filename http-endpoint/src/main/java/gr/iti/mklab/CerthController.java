package gr.iti.mklab;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@Controller
@RequestMapping("/certh-controller")
public class CerthController {

    @RequestMapping(value = "/assessment/{id}/add", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String addTopology(@PathVariable(value = "id") String id,
                              @RequestParam(value = "type", required = true, defaultValue = "ind") String type,
                              @RequestParam(value = "threshold", required = false, defaultValue = "0.9") double threshold) throws Exception {
        Process ps = Runtime.getRuntime().exec(new String[]{"/opt/apache-storm-0.9.1-incubating/bin/storm",
                "jar", "/usr/reveal-storm-modules.jar",
                "itinno.example.ExampleJavaSocialMediaStormTopologyRunner",
                "-mode", "distributed", "-assessmentid", id, "-type", type, "-threshold", String.valueOf(threshold)});
        ps.waitFor();
        java.io.InputStream is = ps.getInputStream();
        byte b[] = new byte[is.available()];
        is.read(b, 0, b.length);
        return new String(b);
    }

    @RequestMapping(value = "/assessment/{id}/remove", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String killTopology(@PathVariable(value = "id") String id) throws Exception {
        Process ps = Runtime.getRuntime().exec(new String[]{"/opt/apache-storm-0.9.1-incubating/bin/storm",
                "kill", id});
        ps.waitFor();
        java.io.InputStream is = ps.getInputStream();
        byte b[] = new byte[is.available()];
        is.read(b, 0, b.length);
        return new String(b);
    }

    public static void main(String[] args) throws Exception {
        Process ps = Runtime.getRuntime().exec(new String[]{"storm", "jar", "/home/kandreadou/docker_storm/reveal-storm-modules.jar",
                "itinno.example.ExampleJavaSocialMediaStormTopologyRunner",
                "-mode", "distributed", "-assessmentid", "testtest"});
        ps.waitFor();
        java.io.InputStream is = ps.getInputStream();
        byte b[] = new byte[is.available()];
        is.read(b, 0, b.length);
        System.out.println(new String(b));
    }
}