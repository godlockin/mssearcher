package com.common.constants;

import lombok.Getter;

@Getter
public enum ResultEnum {

    MEANINGLESS(-1, "无意义")
    , SUCCESS(1, "成功")
    , FAILURE(0, "失败")
    , ES_CLIENT_INIT(11, "ES服务链接失败")
    , ES_CLIENT_BULK_COMMIT(12, "ES Bulk 提交失败")
    , ES_CLIENT_CLOSE(13, "ES Client 关闭失败")
    , ES_QUERY(14, "ES 检索失败")
    , ES_CALL(15, "ES 访问")
    , PARAMETER_CHECK(21, "参数校验失败")
    , REMOTE_QUERY(31, "远程请求失败")
    , ILLEGAL_METHOD(41, "不合理的方法使用")
    ;

    private final int code;
    private final String message;

    ResultEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public static ResultEnum getByCode(int code) {
        for (ResultEnum value : ResultEnum.values()) {
            if (code == value.getCode()) {
                return value;
            }
        }
        return MEANINGLESS;
    }
}
