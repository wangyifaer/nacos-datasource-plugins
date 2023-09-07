package com.alibaba.nacos.plugin.datasource.impl.base;

import com.alibaba.nacos.plugin.datasource.dialect.DatabaseDialect;
import com.alibaba.nacos.plugin.datasource.impl.mysql.HistoryConfigInfoMapperByMySql;
import com.alibaba.nacos.plugin.datasource.manager.DatabaseDialectManager;

/**
 * @Description :
 * @Date : 2023/9/7
 * @Author : WangYiFa
 */
public class BaseHistoryConfigInfoMapper extends HistoryConfigInfoMapperByMySql {

    private DatabaseDialect databaseDialect;

    public BaseHistoryConfigInfoMapper() {
        databaseDialect = DatabaseDialectManager.getInstance().getDialect(getDataSource());
    }

    @Override
    public String pageFindConfigHistoryFetchRows(int pageNo, int pageSize) {
        return databaseDialect.getLimitPageSql("SELECT nid,data_id,group_id,tenant_id,app_name,src_ip,src_user,op_type,gmt_create,gmt_modified FROM his_config_info WHERE data_id = ? AND group_id = ? AND tenant_id = ? ORDER BY nid DESC", pageNo, pageSize);
    }
}
