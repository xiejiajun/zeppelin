package org.apache.zeppelin.rest;



import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.rest.message.ResultExportRequest;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.apache.zeppelin.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.io.IOException;

/**
 * 用于执行结果导出是埋点回调的Api:自己新增的
 * @author xiejiajun
 */
@Path("/result")
@Produces("application/json")
public class ResultExportHookRestApi {

    private static final Logger LOG = LoggerFactory.getLogger(ResultExportHookRestApi.class);



    /**
     * Clone note REST API
     *
     * @return JSON with status.OK
     * @throws IOException,IllegalArgumentException
     */
    @POST
    @Path("/export")
    @ZeppelinApi
    public Response exportResult(String message)
            throws IllegalArgumentException {
        LOG.info("export result by JSON {}", message);

        ResultExportRequest request = ResultExportRequest.fromJson(message);
        String opUser = null;
        if (request != null) {
            opUser = request.getUsername();
        }
        AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
        String loginUser = subject.getUser();

        LOG.info("=======================================start export result=============================");
        LOG.info("export result user:{},loginUser:{}, 段落脚本:{}",opUser,loginUser,request.getParagraph());
        LOG.info("=======================================end export result ==============================");

        return new JsonResponse<>(Response.Status.OK, "").build();
    }
}
