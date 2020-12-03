package org.apache.zeppelin.rest.dto;

import com.google.gson.Gson;
import org.apache.zeppelin.common.JsonSerializable;

/**
 * @author xiejiajun
 */
public class ResultExportAuditDto implements JsonSerializable {

    private static final Gson gson = new Gson();

    private String requestUser;

    private String loginUser;

    private String paragraph;


    @Override
    public String toJson() {
        return gson.toJson(this);
    }

    public static class Builder{

        private String requestUser;

        private String loginUser;

        private String paragraph;

        public Builder setRequestUser(String requestUser){
            this.requestUser = requestUser;
            return this;
        }

        public Builder setLoginUser(String loginUser){
            this.loginUser = loginUser;
            return this;
        }

        public Builder setParagraph(String paragraph){
            this.paragraph = paragraph;
            return this;
        }

        public ResultExportAuditDto build(){
            ResultExportAuditDto dto = new ResultExportAuditDto();
            dto.loginUser = this.loginUser;
            dto.paragraph = this.paragraph;
            dto.requestUser = this.requestUser;
            return dto;
        }
    }
}
