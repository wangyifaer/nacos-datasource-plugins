/*
 * Copyright 1999-2022 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.plugin.datasource.impl.base;


import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.alibaba.nacos.plugin.datasource.dialect.DatabaseDialect;
import com.alibaba.nacos.plugin.datasource.impl.mysql.ConfigInfoBetaMapperByMySql;
import com.alibaba.nacos.plugin.datasource.manager.DatabaseDialectManager;

/**
 * The base implementation of ConfigInfoBetaMapper.
 *
 * @author Long Yu
 **/
public class BaseConfigInfoBetaMapper extends ConfigInfoBetaMapperByMySql {
    
    private DatabaseDialect databaseDialect;
    
    public BaseConfigInfoBetaMapper() {
        databaseDialect = DatabaseDialectManager.getInstance().getDialect(getDataSource());
    }

    public String getLimitPageSqlWithOffset(String sql,int startRow, int pageSize) {
        return databaseDialect.getLimitPageSqlWithOffset(sql, startRow, pageSize);
    }
    
    @Override
    public String findAllConfigInfoBetaForDumpAllFetchRows(int startRow, int pageSize) {
        String sqlInner = getLimitPageSqlWithOffset("SELECT id FROM config_info_beta  ORDER BY id ",startRow, pageSize);
        return  " SELECT t.id,data_id,group_id,tenant_id,app_name,content,md5,gmt_modified,beta_ips,encrypted_data_key "
                + " FROM ( " + sqlInner + "  )"
                + "  g, config_info_beta t WHERE g.id = t.id ";
    }
    
   
    
}
