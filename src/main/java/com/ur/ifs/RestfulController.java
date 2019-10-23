package com.ur.ifs;

import static com.ur.ifs.CompileDeployUnit.compileDeploy;
import static com.ur.ifs.CompileDeployUnit.compileDeployWithOptions;
import static com.ur.ifs.CompileDeployUnit.undeployAll;
import static com.ur.ifs.CompileDeployUnit.undeployOne;

import java.io.IOException;
import java.util.Arrays;

import javax.servlet.http.HttpServletResponse;

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
    public ResponseEntity compileDeployStatement(@RequestBody JSONObject jsonStatement) throws IOException {
        String statement;
        try {
            statement = jsonStatement.get("statement").toString();
        } catch (NullPointerException npe) {
            return ResponseEntity.badRequest().body("Please provide a key EPL statement");
            // response.sendError(400, "JSONObject must contain key statement!");
            // return "JSONObject must contain key statement!";
        }
        LOG.info("Received Statement: " + statement);
        EPDeployment epd;
        try {
            epd = compileDeploy(ep.getRuntime(), statement);
        } catch (Exception e) {
            // response.sendError(400, e.getMessage());
            LOG.error(e.getMessage());
            return ResponseEntity.badRequest().body(e.getMessage());
            // return e.getMessage();
        }
        jsonStatement.put("deploymentId", epd.getDeploymentId());
        jsonStatement.put("deploymentIdDependencies", Arrays.asList(epd.getDeploymentIdDependencies()));
        LOG.info("Statement successfully deployed into Runtime with DeploymentId: " + epd.getDeploymentId() + " and DeploymentIdDependencies: " + Arrays.toString(epd.getDeploymentIdDependencies()));
        return ResponseEntity.ok(jsonStatement);
        // response.setContentType("application/json");
        // return jsonStatement.toString();
        // return "Statement successfully deployed into Runtime with DeploymentId: " + epd.getDeploymentId() + " and DeploymentIdDependencies: " + Arrays.toString(epd.getDeploymentIdDependencies());
    }

    @PutMapping("/statement/{id}")
    public String updateStatement(@PathVariable String id, @RequestBody JSONObject jsonStatement, HttpServletResponse response) throws IOException {
        String statement;
        try {
            statement = jsonStatement.get("statement").toString();
        } catch (NullPointerException npe) {
            response.sendError(400, "JSONObject must contain key statement!");
            return "JSONObject must contain key statement!";
        }
        try {
            undeployOne(ep.getRuntime(), id);
        } catch (Exception e) {
            response.sendError(400, "You can not update a Statement with Dependencies! " + e.getMessage());
            LOG.error("You can not update a Statement with Dependencies! " + e.getMessage());
            return "You can not update a Statement with Dependencies! " + e.getMessage();
        }
        EPDeployment epd;
        DeploymentOptions options = new DeploymentOptions();
        options.setDeploymentId(id);
        try {
            epd = compileDeployWithOptions(ep.getRuntime(), statement, options);
        } catch (Exception e) {
            response.sendError(400, "Updated Statement is not valid, redeploying old Statement! " + e.getMessage());
            LOG.error("Updated Statement is not valid, redeploying old Statement! " + e.getMessage());
            return "Updated Statement is not valid, redeploying old Statement! " + e.getMessage();
        }
        jsonStatement.put("deploymentId", epd.getDeploymentId());
        jsonStatement.put("deploymentIdDependencies", Arrays.asList(epd.getDeploymentIdDependencies()));
        LOG.info("Updated Statement successfully deployed into Runtime with DeploymentId: " + epd.getDeploymentId() + " and DeploymentIdDependencies: " + Arrays.toString(epd.getDeploymentIdDependencies()));
        response.setContentType("application/json");
        return jsonStatement.toString();
   }

    @DeleteMapping("/statement/{id}")
    public String undeployDeleteStatement(@PathVariable String id, HttpServletResponse response) throws IOException {
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
            response.sendError(400, e.getMessage());
            LOG.error(e.getMessage());
            return e.getMessage();
        }
        LOG.info("Statement with DeploymentId: " + id + " undeployed from Runtime and deleted from Collection!");
        return "Statement with DeploymentId: " + id + " undeployed from Runtime and deleted from Collection!";
    }

    @DeleteMapping("/statement/all")
    public String undeployDeleteAllStatements(HttpServletResponse response) throws IOException {
        try {
            undeployAll(ep.getRuntime());
        } catch (Exception e) {
            response.sendError(500, e.getMessage());
            LOG.error(e.getMessage());
            return e.getMessage();
        }
        LOG.info("Undeployed all Statements from Runtime and deleted from Collection " + STATEMENT_COLLECTION_NAME + "!");
        return "Undeployed all Statements from Runtime and deleted from Collection " + STATEMENT_COLLECTION_NAME + "!";
    }
}