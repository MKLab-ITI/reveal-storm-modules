package gr.iti.mklab;

import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.thrift7.TException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Controller
@RequestMapping("/certh-controller")
public class CerthController {

    @RequestMapping(value = "/assessment/{id}/add", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public RevealResponse addTopology(@PathVariable(value = "id") String id,
                                      @RequestParam(value = "type", required = true, defaultValue = "ind") String type,
                                      @RequestParam(value = "threshold", required = false, defaultValue = "0.9") double threshold) throws RevealException {
        try {
            Process ps = Runtime.getRuntime().exec(new String[]{"/opt/apache-storm-0.9.1-incubating/bin/storm",
                    "jar", "/usr/reveal-storm-modules.jar",
                    "itinno.example.ExampleJavaSocialMediaStormTopologyRunner",
                    "-mode", "distributed", "-assessmentid", id, "-type", type, "-threshold", String.valueOf(threshold)});
            ps.waitFor();
            java.io.InputStream is = ps.getInputStream();
            byte b[] = new byte[is.available()];
            is.read(b, 0, b.length);
            return new RevealResponse(true, new String(b));
        } catch (Exception e) {
            throw new RevealException("Error when creating topology " + id, e);
        }
    }

    @RequestMapping(value = "/assessment/{id}/remove", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public RevealResponse killTopology(@PathVariable(value = "id") String id) throws RevealException {
        try {
            Process ps = Runtime.getRuntime().exec(new String[]{"/opt/apache-storm-0.9.1-incubating/bin/storm",
                    "kill", id});
            ps.waitFor();
            java.io.InputStream is = ps.getInputStream();
            byte b[] = new byte[is.available()];
            is.read(b, 0, b.length);
            return new RevealResponse(true, new String(b));
        } catch (Exception e) {
            throw new RevealException("Error when killing topology " + id, e);
        }
    }

    @RequestMapping(value = "/assessment/{id}/status", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public TopologySummary getTopologyStatus(@PathVariable(value = "id") String id) throws RevealException {

        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(Utils.readDefaultConfig());
        List<TopologySummary> topologies;
        try {
            topologies = nimbusClient.getClient().getClusterInfo().get_topologies();
        } catch (TException e) {
            throw new RevealException("No nimbus client found", e);
        }
        for (TopologySummary sum : topologies) {
            if (sum.get_name().equals(id))
                return sum;
        }
        throw new RevealException("No topology found with the specified id", null);
    }

    @RequestMapping(value = "/assessment/status", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<TopologySummary> getStatus() throws RevealException {

        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(Utils.readDefaultConfig());
        try {
            return nimbusClient.getClient().getClusterInfo().get_topologies();
        } catch (TException e) {
            throw new RevealException("No nimbus client found", e);
        }
    }

    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler(RevealException.class)
    @ResponseBody
    public RevealException handleCustomException(RevealException ex) {
        return ex;
    }

    public static void main(String[] args) throws Exception {
        /*Process ps = Runtime.getRuntime().exec(new String[]{"storm", "jar", "/home/kandreadou/docker_storm/reveal-storm-modules.jar",
                "itinno.example.ExampleJavaSocialMediaStormTopologyRunner",
                "-mode", "distributed", "-assessmentid", "testtest"});
        ps.waitFor();
        java.io.InputStream is = ps.getInputStream();
        byte b[] = new byte[is.available()];
        is.read(b, 0, b.length);
        System.out.println(new String(b));*/
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(Utils.readDefaultConfig());
        List<TopologySummary> topologies = nimbusClient.getClient().getClusterInfo().get_topologies();
        int m = 5;
    }
}