package com.ur.ifs;

import static com.ur.ifs.CompileDeployUnit.compileDeploy;
import static com.ur.ifs.CompileDeployUnit.compileDeployWithOptions;
import static com.ur.ifs.CompileDeployUnit.undeployAll;
import static com.ur.ifs.CompileDeployUnit.undeployOne;

import java.io.IOException;
import java.util.Arrays;

import com.espertech.esper.runtime.client.DeploymentOptions;
import com.espertech.esper.runtime.client.EPDeployment;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@CrossOrigin
@RestController
@RequestMapping
@SuppressWarnings("unchecked call")
/*Funktionsweise ist ausführlich im Kapitel Implementierung beschrieben*/
public class RestfulController {

    static final String STATEMENT_COLLECTION_NAME = "StatementCollection";
    private static final Logger LOG = LoggerFactory.getLogger(RestfulController.class);
    @Autowired
    private EventProcessor ep;
    @Autowired
    private MongoDatabase mongoDatabase;

    @PostMapping( value = "/statement", produces = "application/json" )
    public @ResponseBody ResponseEntity<JSONObject> compileDeployStatement(@RequestBody JSONObject jsonStatement) throws IOException {
        try {
            jsonStatement.get("statement").toString();
        } catch (NullPointerException npe) {
            jsonStatement.put("error", "Please provide a key EPL statement");
            return ResponseEntity.badRequest().body(jsonStatement);
        }
        LOG.info("Received Statement: " + jsonStatement.get("statement").toString());
        EPDeployment epd;
        try {
            epd = compileDeploy(ep.getRuntime(), jsonStatement.get("statement").toString());
        } catch (Exception e) {
            LOG.error(e.getMessage());
            jsonStatement.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(jsonStatement);
        }
        jsonStatement.put("deploymentId", epd.getDeploymentId());
        jsonStatement.put("deploymentIdDependencies", Arrays.asList(epd.getDeploymentIdDependencies()));
        LOG.info("Statement successfully deployed into Runtime with DeploymentId: " + epd.getDeploymentId() + " and DeploymentIdDependencies: " + Arrays.toString(epd.getDeploymentIdDependencies()));
        return ResponseEntity.ok(jsonStatement);
    }

    @PutMapping("/statement/{id}")
    public @ResponseBody ResponseEntity<JSONObject> updateStatement(@PathVariable String id, @RequestBody JSONObject jsonStatement) throws IOException {
        try {
            jsonStatement.get("statement").toString();
        } catch (NullPointerException npe) {
            jsonStatement.put("error", "Please provide a key EPL statement");
            return ResponseEntity.badRequest().body(jsonStatement);
        }
        try {
            undeployOne(ep.getRuntime(), id);
        } catch (Exception e) {
            LOG.error("You can not update a Statement with Dependencies! " + e.getMessage());
            jsonStatement.put("error","You can not update a Statement with Dependencies! " + e.getMessage());
            return ResponseEntity.badRequest().body(jsonStatement);
        }
        EPDeployment epd;
        DeploymentOptions options = new DeploymentOptions();
        options.setDeploymentId(id);
        try {
            epd = compileDeployWithOptions(ep.getRuntime(), jsonStatement.get("statement").toString(), options);
        } catch (Exception e) {
            LOG.error("Updated Statement is not valid, redeploying old Statement! " + e.getMessage());
            jsonStatement.put("error","Updated Statement is not valid, redeploying old Statement! " + e.getMessage());
            return ResponseEntity.badRequest().body(jsonStatement);

        }
        jsonStatement.put("deploymentId", epd.getDeploymentId());
        jsonStatement.put("deploymentIdDependencies", Arrays.asList(epd.getDeploymentIdDependencies()));
        LOG.info("Updated Statement successfully deployed into Runtime with DeploymentId: " + epd.getDeploymentId() + " and DeploymentIdDependencies: " + Arrays.toString(epd.getDeploymentIdDependencies()));
        return ResponseEntity.ok(jsonStatement);
   }

    @DeleteMapping("/statement/{id}")
    public @ResponseBody ResponseEntity<String> undeployDeleteStatement(@PathVariable String id) throws IOException {
        Document findDocument = new Document();
        try {
            ObjectId objectId = new ObjectId(id);
            findDocument.append("_id", objectId);
        } catch (Exception e) {
            findDocument.append("deploymentId", id);
        }
        try {
            undeployOne(ep.getRuntime(), id);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return ResponseEntity.badRequest().body(e.getMessage());
        }
        LOG.info("Statement with DeploymentId: " + id + " undeployed from Runtime and deleted from Collection!");
        return ResponseEntity.ok("Statement with DeploymentId: " + id + " undeployed from Runtime and deleted from Collection!");
    }

    @DeleteMapping("/statement/all")
    public @ResponseBody ResponseEntity<String> undeployDeleteAllStatements() throws IOException {
        try {
            undeployAll(ep.getRuntime());
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return ResponseEntity.status(500).body(e.getMessage());
        }
        LOG.info("Undeployed all Statements from Runtime and deleted from Collection " + STATEMENT_COLLECTION_NAME + "!");
        return ResponseEntity.ok("Undeployed all Statements from Runtime and deleted from Collection " + STATEMENT_COLLECTION_NAME + "!");
    }
}