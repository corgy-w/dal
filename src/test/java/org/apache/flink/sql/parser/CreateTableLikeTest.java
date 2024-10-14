/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.ddl.SqlCreateTableLike;
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.sql.parser.ddl.SqlTableLike.FeatureOption;
import org.apache.flink.sql.parser.ddl.SqlTableLike.MergingStrategy;
import org.apache.flink.sql.parser.ddl.SqlTableLike.SqlTableLikeOption;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.empty;

/**
 * Tests for parsing and validating {@link SqlTableLike} clause.
 */
class CreateTableLikeTest {

    @Test
    void testNoOptions() throws Exception {
        SqlNode actualNode =
                createFlinkParser("CREATE TABLE t (\n" + "   a STRING\n" + ")\n" + "LIKE b")
                        .parseStmt();

        assertThat(actualNode)
                .satisfies(matching(hasLikeClause(allOf(pointsTo("b"), hasNoOptions()))));
    }

    @Test
    public void testOptions() throws Exception {
        String sql =
                "select" +
                        "        t1.id\n" +
                        "        , t1.report_no\n" +
                        "        , t1.status\n" +
                        "        , coalesce(status.meta_value, t1.status) status_name\n" +
                        "        , t1.source\n" +
                        "        , coalesce(source.meta_value, t1.source) source_name\n" +
                        "        , t1.archive_no\n" +
                        "        , t1.seppage_path_file_name\n" +
                        "        , t1.channel_application_id\n" +
                        "        , t1.image_no\n" +
                        "        , t1.seppage_path\n" +
                        "        , coalesce(t1.loss_type, 'illness') loss_type -- 系统默认值为 illness\n" +
                        "        , coalesce(loss_type.meta_value, t1.loss_type) loss_type_name\n" +
                        "        , t1.loss_code\n" +
                        "        , t3.loss_code_split loss_code_name -- 出险原因名称\n" +
                        "        , t1.insurance_name\n" +
                        "        , t1.insurance_ceti_no\n" +
                        "        , t1.insurance_ceti_type\n" +
                        "        , t1.accident_date\n" +
                        "        , t1.accident_area\n" +
                        "        , t4.area_name    accident_area_name--    出现区域名称\n" +
                        "        , t1.accident_place\n" +
                        "        , t1.accident_process\n" +
                        "        , t1.reporter\n" +
                        "        , t1.reporter_insured_relation\n" +
                        "        , coalesce(reporter_insured_relation.meta_value, t1.reporter_insured_relation) reporter_insured_relation_name\n" +
                        "        , t1.tel_no\n" +
                        "        , t1.mobile_no\n" +
                        "        , t1.claim_amount\n" +
                        "        , t1.claim_currency\n" +
                        "        , t1.remark\n" +
                        "        , t1.handler\n" +
                        "        , t1.reported_date\n" +
                        "        , t1.death_date\n" +
                        "        , t1.close_date\n" +
                        "        , t1.creator\n" +
                        "        , t1.creator_no\n" +
                        "        , t1.gmt_created\n" +
                        "        , t1.modifier\n" +
                        "        , t1.gmt_modified\n" +
                        "        , t1.extra_info\n" +
                        "        , extra_info.masterInsuredInfo extra_info__masterinsuredinfo -- 主被保人\n" +
                        "        , extra_info.isNeedClaimDivision extra_info__isneedclaimdivision -- 是否需要理赔分割单类型\n" +
                        "        , extra_info.planCodeList extra_info__plancodelist -- 滴滴报案时的产品组合\n" +
                        "        , extra_info.materialSmsSend extra_info__materialsmssend --  补充材料发送成功标记\n" +
                        "        , extra_info.customerNo extra_info__customerno  -- 滴滴报案客户号\n" +
                        "        , extra_info.accidentDate extra_info__accidentdate --  出险时间\n" +
                        "        , extra_info.sysLocMobileNo extra_info__syslocmobileno --   系统自动定位的手机号\n" +
                        "        , extra_info.syncVersion extra_info__syncversion --  同步搜索数据版本\n" +
                        "        , extra_info.claimReturnChannelKey extra_info__claimreturnchannelkey --  案件回传腾讯商保标记\n" +
                        "        , extra_info.importDataSource extra_info__importDataSource --  案件导入来源\n" +
                        "        , t1.is_deleted\n" +
                        "        , t1.allow_dispatch\n" +
                        "        , t1.istpadispatch\n" +
                        "        , t1.email\n" +
                        "        , (case when t1.gender is null and t1.insurance_ceti_type = 'I'\n" +
                        "            then GET_IDCARD_SEX(t1.insurance_ceti_no)\n" +
                        "           end) gender\n" +
                        "        , (case when t1.birth_day is null and t1.insurance_ceti_type = 'I'\n" +
                        "            then get_idcard_birthday(t1.insurance_ceti_no)\n" +
                        "           end) birth_day\n" +
                        "        , t1.channel_batch_no\n" +
                        "        , t1.channel_report_no\n" +
                        "        , t1.adjustment_date\n" +
                        "        , t1.tpa_source\n" +
                        "        , coalesce(tpa_source.meta_value, t1.tpa_source) tpa_source_name\n" +
                        "        , t1.master_insured_cert_no\n" +
                        "        , t1.master_insured_cert_type\n" +
                        "        , coalesce(t1.is_campaign, '0') is_campaign -- 系统默认为非滴滴案件\n" +
                        "        , t1.diagnosis_date\n" +
                        "        , t1.is_attachment\n" +
                        "        , t1.is_vip\n" +
                        "        , t1.elapsed_time\n" +
                        "        , t1.disallow_batch_close\n" +
                        "        , t1.department\n" +
                        "        , t1.has_potential_missing_policy\n" +
                        "        , t1.potential_missing_policy\n" +
                        "        , t1.lawsuit\n" +
                        "        , t1.has_tried_to_dispatch_tpa\n" +
                        "        , t1.auto_dispatch_status\n" +
                        "        , t1.auto_dispatch_fail_count\n" +
                        "        , t1.error_type\n" +
                        "        , t1.mdp_hospital_name\n" +
                        "        , t1.mdp_hospital_code\n" +
                        "        , t1.mdp_invoice_nos\n" +
                        "        , t1.order_no\n" +
                        "        , t1.claim_date\n" +
                        "        , t1.one_notice_date\n" +
                        "        , t1.material_complete_date\n" +
                        "        , t1.case_mark\n" +
                        "        , coalesce(case_mark.meta_value, t1.case_mark) case_mark_name\n" +
                        "        , t1.is_preclaim_adjustment\n" +
                        "        , t1.is_internal\n" +
                        "        , t1.is_preclaim\n" +
                        "        , t1.cancel_operator\n" +
                        "        , t1.cancel_review_operator\n" +
                        "        , t1.appeal_date\n" +
                        "        , t1.appeal_no\n" +
                        "        , t1.first_close_date\n" +
                        "        , t1.case_tag\n" +
                        "        , t1.is_special_drugs\n" +
                        "        , t2.source_policy_no -- 保单号\n" +
                        "        , t2.source_policy_id -- 保单ID\n" +
                        "        , t2.repeat_nos --   注销原因为客户重复报案时选择的重复案件号\n" +
                        "        , t2.no_claim_sms -- 是否不需要发送理赔短信 1：不需要， 0/null：需要\n" +
                        "        , t2.no_claim_email --   是否不需要发送理赔邮件 1：不需要， 0/null：需要\n" +
                        "        , t2.no_customer_visit --    是否不需要客服回访 1：不需要， 0/null：需要\n" +
                        "        , t2.risk_remark --  风险备注\n" +
                        "        , risk_remark.riskName risk_remark__riskName --  风险备注\n" +
                        "        , risk_remark.riskDetail risk_remark__riskDetail --  风险备注\n" +
                        "        , risk_remark.riskResult risk_remark__riskResult --  风险备注\n" +
                        "        , (case when t1.etl_source_table='za_ha_prd.ods_wjs_ha_claim_application' then 'yjx'\n" +
                        "            when t1.etl_source_table='za_ha_prd.ods_ha_vhf_ha_claim_application' then 'yjx-vhf'\n" +
                        "        end) pt2\n" +
                        "        , coalesce(case_tag.meta_value,t1.case_tag) as case_tag_name\n" +
                        "        , t1.catastrophe_code\n" +
                        "        , t1.catastrophe_name\n" +
                        "        , t1.elapsed_time_seconds\n" +
                        "        , decode(t1.sub_status,'caseReview_first','理赔一核中','caseReview_secondary','理赔二核中','caseReview_operationManagement','运管复核中',t1.sub_status) as sub_status\n" +
                        "    from za_ha_prd.ods_ha_claim_application_all t1\n" +
                        "    left outer join za_ha_prd.ods_ha_claim_application_extra_info_all t2\n" +
                        "        on t1.id = t2.claim_application_id\n" +
                        "            and t2.pt = '${bizdate}000000'\n" +
                        "            and t2.is_deleted = 'N'\n" +
                        "    left outer join (select\n" +
                        "                        t1.report_no\n" +
                        "                        , wm_concat(',', loss_code_name) loss_code_split\n" +
                        "                    from edw_ha_claim_application__loss_code t1\n" +
                        "                    where t1.pt='${bizdate}000000'\n" +
                        "                    group by\n" +
                        "                        t1.report_no) t3\n" +
                        "        on t1.report_no = t3.report_no\n" +
                        "    left outer join tmp_ha_claim_application__accident_area t4\n" +
                        "        on t4.pt='${bizdate}000000'\n" +
                        "            and t1.report_no = t4.report_no\n" +
                        "    left outer join pub_ha_code_value_dim status\n" +
                        "        on t1.status = status.src_key\n" +
                        "            and status.tb_name = 'za_ha_prd.ods_ha_claim_application_all'\n" +
                        "            and status.cl_name = 'status'\n" +
                        "    left outer join pub_ha_code_value_dim case_mark\n" +
                        "        on t1.case_mark = case_mark.src_key\n" +
                        "            and case_mark.tb_name = 'za_ha_prd.ods_ha_claim_application_all'\n" +
                        "            and case_mark.cl_name = 'case_mark'\n" +
                        "    left outer join pub_ha_code_value_dim tpa_source\n" +
                        "        on t1.tpa_source = tpa_source.src_key\n" +
                        "            and tpa_source.tb_name = 'za_ha_prd.ods_ha_claim_application_all'\n" +
                        "            and tpa_source.cl_name = 'tpa_source'\n" +
                        "    left outer join pub_ha_code_value_dim source\n" +
                        "        on t1.source = source.src_key\n" +
                        "            and source.tb_name = 'za_ha_prd.ods_ha_claim_application_all'\n" +
                        "            and source.cl_name = 'source'\n" +
                        "    left outer join pub_ha_code_value_dim loss_type\n" +
                        "        on coalesce(t1.loss_type, 'illness')= loss_type.src_key\n" +
                        "            and loss_type.tb_name = 'za_ha_prd.ods_ha_claim_application_all'\n" +
                        "            and loss_type.cl_name = 'loss_type'\n" +
                        "    left outer join pub_ha_code_value_dim reporter_insured_relation\n" +
                        "        on t1.reporter_insured_relation = reporter_insured_relation.src_key\n" +
                        "            and reporter_insured_relation.tb_name = 'za_ha_prd.ods_ha_claim_application_all'\n" +
                        "            and reporter_insured_relation.cl_name = 'reporter_insured_relation'\n" +
                        "    lateral view json_tuple(t1.extra_info\n" +
                        "                            , 'masterInsuredInfo', 'isNeedClaimDivision', 'planCodeList', 'materialSmsSend', 'customerNo'\n" +
                        "                            , 'accidentDate', 'sysLocMobileNo', 'syncVersion', 'claimReturnChannelKey', 'importDataSource') extra_info\n" +
                        "                            as masterInsuredInfo, isNeedClaimDivision, planCodeList, materialSmsSend, customerNo\n" +
                        "                                , accidentDate, sysLocMobileNo, syncVersion, claimReturnChannelKey, importDataSource\n" +
                        "    lateral view json_tuple(t2.risk_remark\n" +
                        "                            , 'riskName', 'riskDetail', 'riskResult') risk_remark\n" +
                        "                            as riskName, riskDetail, riskResult\n" +
                        "    -- case_tag转码\n" +
                        "    left join za_ha_prd.pub_ha_code_value_dim case_tag\n" +
                        "        on t1.case_tag = case_tag.src_key\n" +
                        "            and case_tag.tb_name='za_ha_prd.ods_ha_claim_application_all'\n" +
                        "            and case_tag.cl_name='case_tag'\n" +
                        "    where t1.pt = '${bizdate}000000'";
        SqlNode actualNode = createFlinkParser(sql)
                .parseStmt();
        System.out.println(actualNode);
    }

    @Test
    public void testSelect() throws SqlParseException {
        String sql = "from(\n" +
                "    select /*+mapjoin(status,source,loss_type,reporter_insured_relation,t3,t4,case_mark,tpa_source)*/\n" +
                "        t1.id\n" +
                "        , t1.report_no\n" +
                "        , t1.status\n" +
                "        , coalesce(status.meta_value, t1.status) status_name\n" +
                "        , t1.source\n" +
                "        , coalesce(source.meta_value, t1.source) source_name\n" +
                "        , t1.archive_no\n" +
                "        , t1.seppage_path_file_name\n" +
                "        , t1.channel_application_id\n" +
                "        , t1.image_no\n" +
                "        , t1.seppage_path\n" +
                "        , coalesce(t1.loss_type, 'illness') loss_type -- 系统默认值为 illness\n" +
                "        , coalesce(loss_type.meta_value, t1.loss_type) loss_type_name\n" +
                "        , t1.loss_code\n" +
                "        , t3.loss_code_split loss_code_name -- 出险原因名称\n" +
                "        , t1.insurance_name\n" +
                "        , t1.insurance_ceti_no\n" +
                "        , t1.insurance_ceti_type\n" +
                "        , t1.accident_date\n" +
                "        , t1.accident_area\n" +
                "        , t4.area_name    accident_area_name--    出现区域名称\n" +
                "        , t1.accident_place\n" +
                "        , t1.accident_process\n" +
                "        , t1.reporter\n" +
                "        , t1.reporter_insured_relation\n" +
                "        , coalesce(reporter_insured_relation.meta_value, t1.reporter_insured_relation) reporter_insured_relation_name\n" +
                "        , t1.tel_no\n" +
                "        , t1.mobile_no\n" +
                "        , t1.claim_amount\n" +
                "        , t1.claim_currency\n" +
                "        , t1.remark\n" +
                "        , t1.handler\n" +
                "        , t1.reported_date\n" +
                "        , t1.death_date\n" +
                "        , t1.close_date\n" +
                "        , t1.creator\n" +
                "        , t1.creator_no\n" +
                "        , t1.gmt_created\n" +
                "        , t1.modifier\n" +
                "        , t1.gmt_modified\n" +
                "        , t1.extra_info\n" +
                "        , extra_info.masterInsuredInfo extra_info__masterinsuredinfo -- 主被保人\n" +
                "        , extra_info.isNeedClaimDivision extra_info__isneedclaimdivision -- 是否需要理赔分割单类型\n" +
                "        , extra_info.planCodeList extra_info__plancodelist -- 滴滴报案时的产品组合\n" +
                "        , extra_info.materialSmsSend extra_info__materialsmssend --  补充材料发送成功标记\n" +
                "        , extra_info.customerNo extra_info__customerno  -- 滴滴报案客户号\n" +
                "        , extra_info.accidentDate extra_info__accidentdate --  出险时间\n" +
                "        , extra_info.sysLocMobileNo extra_info__syslocmobileno --   系统自动定位的手机号\n" +
                "        , extra_info.syncVersion extra_info__syncversion --  同步搜索数据版本\n" +
                "        , extra_info.claimReturnChannelKey extra_info__claimreturnchannelkey --  案件回传腾讯商保标记\n" +
                "        , extra_info.importDataSource extra_info__importDataSource --  案件导入来源\n" +
                "        , t1.is_deleted\n" +
                "        , t1.allow_dispatch\n" +
                "        , t1.istpadispatch\n" +
                "        , t1.email\n" +
                "        , (case when t1.gender is null and t1.insurance_ceti_type = 'I'\n" +
                "            then GET_IDCARD_SEX(t1.insurance_ceti_no)\n" +
                "           end) gender\n" +
                "        , (case when t1.birth_day is null and t1.insurance_ceti_type = 'I'\n" +
                "            then get_idcard_birthday(t1.insurance_ceti_no)\n" +
                "           end) birth_day\n" +
                "        , t1.channel_batch_no\n" +
                "        , t1.channel_report_no\n" +
                "        , t1.adjustment_date\n" +
                "        , t1.tpa_source\n" +
                "        , coalesce(tpa_source.meta_value, t1.tpa_source) tpa_source_name\n" +
                "        , t1.master_insured_cert_no\n" +
                "        , t1.master_insured_cert_type\n" +
                "        , coalesce(t1.is_campaign, '0') is_campaign -- 系统默认为非滴滴案件\n" +
                "        , t1.diagnosis_date\n" +
                "        , t1.is_attachment\n" +
                "        , t1.is_vip\n" +
                "        , t1.elapsed_time\n" +
                "        , t1.disallow_batch_close\n" +
                "        , t1.department\n" +
                "        , t1.has_potential_missing_policy\n" +
                "        , t1.potential_missing_policy\n" +
                "        , t1.lawsuit\n" +
                "        , t1.has_tried_to_dispatch_tpa\n" +
                "        , t1.auto_dispatch_status\n" +
                "        , t1.auto_dispatch_fail_count\n" +
                "        , t1.error_type\n" +
                "        , t1.mdp_hospital_name\n" +
                "        , t1.mdp_hospital_code\n" +
                "        , t1.mdp_invoice_nos\n" +
                "        , t1.order_no\n" +
                "        , t1.claim_date\n" +
                "        , t1.one_notice_date\n" +
                "        , t1.material_complete_date\n" +
                "        , t1.case_mark\n" +
                "        , coalesce(case_mark.meta_value, t1.case_mark) case_mark_name\n" +
                "        , t1.is_preclaim_adjustment\n" +
                "        , t1.is_internal\n" +
                "        , t1.is_preclaim\n" +
                "        , t1.cancel_operator\n" +
                "        , t1.cancel_review_operator\n" +
                "        , t1.appeal_date\n" +
                "        , t1.appeal_no\n" +
                "        , t1.first_close_date\n" +
                "        , t1.case_tag\n" +
                "        , t1.is_special_drugs\n" +
                "        , t2.source_policy_no -- 保单号\n" +
                "        , t2.source_policy_id -- 保单ID\n" +
                "        , t2.repeat_nos --   注销原因为客户重复报案时选择的重复案件号\n" +
                "        , t2.no_claim_sms -- 是否不需要发送理赔短信 1：不需要， 0/null：需要\n" +
                "        , t2.no_claim_email --   是否不需要发送理赔邮件 1：不需要， 0/null：需要\n" +
                "        , t2.no_customer_visit --    是否不需要客服回访 1：不需要， 0/null：需要\n" +
                "        , t2.risk_remark --  风险备注\n" +
                "        , risk_remark.riskName risk_remark__riskName --  风险备注\n" +
                "        , risk_remark.riskDetail risk_remark__riskDetail --  风险备注\n" +
                "        , risk_remark.riskResult risk_remark__riskResult --  风险备注\n" +
                "        , (case when t1.etl_source_table='za_ha_prd.ods_wjs_ha_claim_application' then 'yjx'\n" +
                "            when t1.etl_source_table='za_ha_prd.ods_ha_vhf_ha_claim_application' then 'yjx-vhf'\n" +
                "        end) pt2\n" +
                "        , coalesce(case_tag.meta_value,t1.case_tag) as case_tag_name\n" +
                "        , t1.catastrophe_code\n" +
                "        , t1.catastrophe_name\n" +
                "        , t1.elapsed_time_seconds\n" +
                "        , decode(t1.sub_status,'caseReview_first','理赔一核中','caseReview_secondary','理赔二核中','caseReview_operationManagement','运管复核中',t1.sub_status) as sub_status\n" +
                "    from za_ha_prd.ods_ha_claim_application_all t1\n" +
                "    left outer join za_ha_prd.ods_ha_claim_application_extra_info_all t2\n" +
                "        on t1.id = t2.claim_application_id\n" +
                "            and t2.pt = '${bizdate}000000'\n" +
                "            and t2.is_deleted = 'N'\n" +
                "    left outer join (select\n" +
                "                        t1.report_no\n" +
                "                        , wm_concat(',', loss_code_name) loss_code_split\n" +
                "                    from edw_ha_claim_application__loss_code t1\n" +
                "                    where t1.pt='${bizdate}000000'\n" +
                "                    group by\n" +
                "                        t1.report_no) t3\n" +
                "        on t1.report_no = t3.report_no\n" +
                "    left outer join tmp_ha_claim_application__accident_area t4\n" +
                "        on t4.pt='${bizdate}000000'\n" +
                "            and t1.report_no = t4.report_no\n" +
                "    left outer join pub_ha_code_value_dim status\n" +
                "        on t1.status = status.src_key\n" +
                "            and status.tb_name = 'za_ha_prd.ods_ha_claim_application_all'\n" +
                "            and status.cl_name = 'status'\n" +
                "    left outer join pub_ha_code_value_dim case_mark\n" +
                "        on t1.case_mark = case_mark.src_key\n" +
                "            and case_mark.tb_name = 'za_ha_prd.ods_ha_claim_application_all'\n" +
                "            and case_mark.cl_name = 'case_mark'\n" +
                "    left outer join pub_ha_code_value_dim tpa_source\n" +
                "        on t1.tpa_source = tpa_source.src_key\n" +
                "            and tpa_source.tb_name = 'za_ha_prd.ods_ha_claim_application_all'\n" +
                "            and tpa_source.cl_name = 'tpa_source'\n" +
                "    left outer join pub_ha_code_value_dim source\n" +
                "        on t1.source = source.src_key\n" +
                "            and source.tb_name = 'za_ha_prd.ods_ha_claim_application_all'\n" +
                "            and source.cl_name = 'source'\n" +
                "    left outer join pub_ha_code_value_dim loss_type\n" +
                "        on coalesce(t1.loss_type, 'illness')= loss_type.src_key\n" +
                "            and loss_type.tb_name = 'za_ha_prd.ods_ha_claim_application_all'\n" +
                "            and loss_type.cl_name = 'loss_type'\n" +
                "    left outer join pub_ha_code_value_dim reporter_insured_relation\n" +
                "        on t1.reporter_insured_relation = reporter_insured_relation.src_key\n" +
                "            and reporter_insured_relation.tb_name = 'za_ha_prd.ods_ha_claim_application_all'\n" +
                "            and reporter_insured_relation.cl_name = 'reporter_insured_relation'\n" +
                "    lateral view json_tuple(t1.extra_info\n" +
                "                            , 'masterInsuredInfo', 'isNeedClaimDivision', 'planCodeList', 'materialSmsSend', 'customerNo'\n" +
                "                            , 'accidentDate', 'sysLocMobileNo', 'syncVersion', 'claimReturnChannelKey', 'importDataSource') extra_info\n" +
                "                            as masterInsuredInfo, isNeedClaimDivision, planCodeList, materialSmsSend, customerNo\n" +
                "                                , accidentDate, sysLocMobileNo, syncVersion, claimReturnChannelKey, importDataSource\n" +
                "    lateral view json_tuple(t2.risk_remark\n" +
                "                            , 'riskName', 'riskDetail', 'riskResult') risk_remark\n" +
                "                            as riskName, riskDetail, riskResult\n" +
                "    -- case_tag转码\n" +
                "    left join za_ha_prd.pub_ha_code_value_dim case_tag\n" +
                "        on t1.case_tag = case_tag.src_key\n" +
                "            and case_tag.tb_name='za_ha_prd.ods_ha_claim_application_all'\n" +
                "            and case_tag.cl_name='case_tag'\n" +
                "    where t1.pt = '${bizdate}000000'\n" +
                ")  t1\n" +
                "-- 意健险高频导入库\n" +
                "insert overwrite table edw_ha_claim_application partition(pt = '${bizdate}000000', pt2 = 'yjx-vhf')\n" +
                "select\n" +
                "    t1.id\n" +
                "    , t1.report_no\n" +
                "    , t1.status\n" +
                "    , t1.status_name\n" +
                "    , t1.source\n" +
                "    , t1.source_name\n" +
                "    , t1.archive_no\n" +
                "    , t1.seppage_path_file_name\n" +
                "    , t1.channel_application_id\n" +
                "    , t1.image_no\n" +
                "    , t1.seppage_path\n" +
                "    , t1.loss_type -- 系统默认值为 illness\n" +
                "    , t1.loss_type_name\n" +
                "    , t1.loss_code\n" +
                "    , t1.loss_code_name -- 出险原因名称\n" +
                "    , t1.insurance_name\n" +
                "    , t1.insurance_ceti_no\n" +
                "    , t1.insurance_ceti_type\n" +
                "    , t1.accident_date\n" +
                "    , t1.accident_area\n" +
                "    , t1.accident_area_name--    出现区域名称\n" +
                "    , t1.accident_place\n" +
                "    , t1.accident_process\n" +
                "    , t1.reporter\n" +
                "    , t1.reporter_insured_relation\n" +
                "    , t1.reporter_insured_relation_name\n" +
                "    , t1.tel_no\n" +
                "    , t1.mobile_no\n" +
                "    , t1.claim_amount\n" +
                "    , t1.claim_currency\n" +
                "    , t1.remark\n" +
                "    , t1.handler\n" +
                "    , t1.reported_date\n" +
                "    , t1.death_date\n" +
                "    , t1.close_date\n" +
                "    , t1.creator\n" +
                "    , t1.creator_no\n" +
                "    , t1.gmt_created\n" +
                "    , t1.modifier\n" +
                "    , t1.gmt_modified\n" +
                "    , t1.extra_info\n" +
                "    , t1.extra_info__masterinsuredinfo -- 主被保人\n" +
                "    , t1.extra_info__isneedclaimdivision -- 是否需要理赔分割单类型\n" +
                "    , t1.extra_info__plancodelist -- 滴滴报案时的产品组合\n" +
                "    , t1.extra_info__materialsmssend --  补充材料发送成功标记\n" +
                "    , t1.extra_info__customerno  -- 滴滴报案客户号\n" +
                "    , t1.extra_info__accidentdate --  出险时间\n" +
                "    , t1.extra_info__syslocmobileno --   系统自动定位的手机号\n" +
                "    , t1.extra_info__syncversion --  同步搜索数据版本\n" +
                "    , t1.extra_info__claimreturnchannelkey --  案件回传腾讯商保标记\n" +
                "    , t1.extra_info__importDataSource --  案件导入来源\n" +
                "    , t1.is_deleted\n" +
                "    , t1.allow_dispatch\n" +
                "    , t1.istpadispatch\n" +
                "    , t1.email\n" +
                "    , t1.gender\n" +
                "    , t1.birth_day\n" +
                "    , t1.channel_batch_no\n" +
                "    , t1.channel_report_no\n" +
                "    , t1.adjustment_date\n" +
                "    , t1.tpa_source\n" +
                "    , t1.tpa_source_name\n" +
                "    , t1.master_insured_cert_no\n" +
                "    , t1.master_insured_cert_type\n" +
                "    , t1.is_campaign -- 系统默认为非滴滴案件\n" +
                "    , t1.diagnosis_date\n" +
                "    , t1.is_attachment\n" +
                "    , t1.is_vip\n" +
                "    , t1.elapsed_time\n" +
                "    , t1.disallow_batch_close\n" +
                "    , t1.department\n" +
                "    , t1.has_potential_missing_policy\n" +
                "    , t1.potential_missing_policy\n" +
                "    , t1.lawsuit\n" +
                "    , t1.has_tried_to_dispatch_tpa\n" +
                "    , t1.auto_dispatch_status\n" +
                "    , t1.auto_dispatch_fail_count\n" +
                "    , t1.error_type\n" +
                "    , t1.mdp_hospital_name\n" +
                "    , t1.mdp_hospital_code\n" +
                "    , t1.mdp_invoice_nos\n" +
                "    , t1.order_no\n" +
                "    , t1.claim_date\n" +
                "    , t1.one_notice_date\n" +
                "    , t1.material_complete_date\n" +
                "    , t1.case_mark\n" +
                "    , t1.case_mark_name\n" +
                "    , t1.is_preclaim_adjustment\n" +
                "    , t1.is_internal\n" +
                "    , t1.is_preclaim\n" +
                "    , t1.cancel_operator\n" +
                "    , t1.cancel_review_operator\n" +
                "    , t1.appeal_date\n" +
                "    , t1.appeal_no\n" +
                "    , t1.first_close_date\n" +
                "    , t1.case_tag\n" +
                "    , t1.is_special_drugs\n" +
                "    , t1.source_policy_no -- 保单号\n" +
                "    , t1.source_policy_id -- 保单ID\n" +
                "    , t1.repeat_nos --   注销原因为客户重复报案时选择的重复案件号\n" +
                "    , t1.no_claim_sms -- 是否不需要发送理赔短信 1：不需要， 0/null：需要\n" +
                "    , t1.no_claim_email --   是否不需要发送理赔邮件 1：不需要， 0/null：需要\n" +
                "    , t1.no_customer_visit --    是否不需要客服回访 1：不需要， 0/null：需要\n" +
                "    , t1.risk_remark --  风险备注\n" +
                "    , t1.risk_remark__riskName --  风险备注\n" +
                "    , t1.risk_remark__riskDetail --  风险备注\n" +
                "    , t1.risk_remark__riskResult --  风险备注\n" +
                "    , t1.case_tag_name --案件标签名称\n" +
                "    , t1.catastrophe_code --巨灾代码\n" +
                "    , t1.catastrophe_name --巨灾代码名称\n" +
                "    , t1.elapsed_time_seconds --经过时间(秒)\n" +
                "    , t1.sub_status --理赔子状态\n" +
                "where pt2 = 'yjx-vhf'\n" +
                ";\n";
        SqlNode actualNode = createFlinkParser(sql)
                .parseStmt();
        System.out.println(actualNode);
    }

    @Test
    public void testInsert() throws SqlParseException {
        String sql = "-- 意健险高频导入库\n" +
                "insert INTO table edw_ha_claim_application \n" +
                "select\n" +
                "    t1.id\n" +
                "    , t1.report_no\n" +
                "    , t1.status\n" +
                "    , t1.status_name\n" +
                "    , t1.source\n" +
                "    , t1.source_name\n" +
                "    , t1.archive_no\n" +
                "    , t1.seppage_path_file_name\n" +
                "    , t1.channel_application_id\n" +
                "    , t1.image_no\n" +
                "    , t1.seppage_path\n" +
                "    , t1.loss_type -- 系统默认值为 illness\n" +
                "    , t1.loss_type_name\n" +
                "    , t1.loss_code\n" +
                "    , t1.loss_code_name -- 出险原因名称\n" +
                "    , t1.insurance_name\n" +
                "    , t1.insurance_ceti_no\n" +
                "    , t1.insurance_ceti_type\n" +
                "    , t1.accident_date\n" +
                "    , t1.accident_area\n" +
                "    , t1.accident_area_name--    出现区域名称\n" +
                "    , t1.accident_place\n" +
                "    , t1.accident_process\n" +
                "    , t1.reporter\n" +
                "    , t1.reporter_insured_relation\n" +
                "    , t1.reporter_insured_relation_name\n" +
                "    , t1.tel_no\n" +
                "    , t1.mobile_no\n" +
                "    , t1.claim_amount\n" +
                "    , t1.claim_currency\n" +
                "    , t1.remark\n" +
                "    , t1.handler\n" +
                "    , t1.reported_date\n" +
                "    , t1.death_date\n" +
                "    , t1.close_date\n" +
                "    , t1.creator\n" +
                "    , t1.creator_no\n" +
                "    , t1.gmt_created\n" +
                "    , t1.modifier\n" +
                "    , t1.gmt_modified\n" +
                "    , t1.extra_info\n" +
                "    , t1.extra_info__masterinsuredinfo -- 主被保人\n" +
                "    , t1.extra_info__isneedclaimdivision -- 是否需要理赔分割单类型\n" +
                "    , t1.extra_info__plancodelist -- 滴滴报案时的产品组合\n" +
                "    , t1.extra_info__materialsmssend --  补充材料发送成功标记\n" +
                "    , t1.extra_info__customerno  -- 滴滴报案客户号\n" +
                "    , t1.extra_info__accidentdate --  出险时间\n" +
                "    , t1.extra_info__syslocmobileno --   系统自动定位的手机号\n" +
                "    , t1.extra_info__syncversion --  同步搜索数据版本\n" +
                "    , t1.extra_info__claimreturnchannelkey --  案件回传腾讯商保标记\n" +
                "    , t1.extra_info__importDataSource --  案件导入来源\n" +
                "    , t1.is_deleted\n" +
                "    , t1.allow_dispatch\n" +
                "    , t1.istpadispatch\n" +
                "    , t1.email\n" +
                "    , t1.gender\n" +
                "    , t1.birth_day\n" +
                "    , t1.channel_batch_no\n" +
                "    , t1.channel_report_no\n" +
                "    , t1.adjustment_date\n" +
                "    , t1.tpa_source\n" +
                "    , t1.tpa_source_name\n" +
                "    , t1.master_insured_cert_no\n" +
                "    , t1.master_insured_cert_type\n" +
                "    , t1.is_campaign -- 系统默认为非滴滴案件\n" +
                "    , t1.diagnosis_date\n" +
                "    , t1.is_attachment\n" +
                "    , t1.is_vip\n" +
                "    , t1.elapsed_time\n" +
                "    , t1.disallow_batch_close\n" +
                "    , t1.department\n" +
                "    , t1.has_potential_missing_policy\n" +
                "    , t1.potential_missing_policy\n" +
                "    , t1.lawsuit\n" +
                "    , t1.has_tried_to_dispatch_tpa\n" +
                "    , t1.auto_dispatch_status\n" +
                "    , t1.auto_dispatch_fail_count\n" +
                "    , t1.error_type\n" +
                "    , t1.mdp_hospital_name\n" +
                "    , t1.mdp_hospital_code\n" +
                "    , t1.mdp_invoice_nos\n" +
                "    , t1.order_no\n" +
                "    , t1.claim_date\n" +
                "    , t1.one_notice_date\n" +
                "    , t1.material_complete_date\n" +
                "    , t1.case_mark\n" +
                "    , t1.case_mark_name\n" +
                "    , t1.is_preclaim_adjustment\n" +
                "    , t1.is_internal\n" +
                "    , t1.is_preclaim\n" +
                "    , t1.cancel_operator\n" +
                "    , t1.cancel_review_operator\n" +
                "    , t1.appeal_date\n" +
                "    , t1.appeal_no\n" +
                "    , t1.first_close_date\n" +
                "    , t1.case_tag\n" +
                "    , t1.is_special_drugs\n" +
                "    , t1.source_policy_no -- 保单号\n" +
                "    , t1.source_policy_id -- 保单ID\n" +
                "    , t1.repeat_nos --   注销原因为客户重复报案时选择的重复案件号\n" +
                "    , t1.no_claim_sms -- 是否不需要发送理赔短信 1：不需要， 0/null：需要\n" +
                "    , t1.no_claim_email --   是否不需要发送理赔邮件 1：不需要， 0/null：需要\n" +
                "    , t1.no_customer_visit --    是否不需要客服回访 1：不需要， 0/null：需要\n" +
                "    , t1.risk_remark --  风险备注\n" +
                "    , t1.risk_remark__riskName --  风险备注\n" +
                "    , t1.risk_remark__riskDetail --  风险备注\n" +
                "    , t1.risk_remark__riskResult --  风险备注\n" +
                "    , t1.case_tag_name --案件标签名称\n" +
                "    , t1.catastrophe_code --巨灾代码\n" +
                "    , t1.catastrophe_name --巨灾代码名称\n" +
                "    , t1.elapsed_time_seconds --经过时间(秒)\n" +
                "    , t1.sub_status --理赔子状态\n";
        SqlParser sqlParser = createFlinkParser(sql);

        SqlNode actualNode = sqlParser.parseStmt();
        System.out.println(actualNode);
    }

    @Test
    public void testInsert2() throws SqlParseException {
        String sql =  "-- 意健险业务库\n" +
                "insert overwrite table edw_ha_claim_application \n" +
                "select\n" +
                "    t1.id\n" +
                "    , t1.report_no\n" +
                "    , t1.status\n" +
                "    , t1.status_name\n" +
                "    , t1.source\n" +
                "    , t1.source_name\n" +
                "    , t1.archive_no\n" +
                "    , t1.seppage_path_file_name\n" +
                "    , t1.channel_application_id\n" +
                "    , t1.image_no\n" +
                "    , t1.seppage_path\n" +
                "    , t1.loss_type -- 系统默认值为 illness\n" +
                "    , t1.loss_type_name\n" +
                "    , t1.loss_code\n" +
                "    , t1.loss_code_name -- 出险原因名称\n" +
                "    , t1.insurance_name\n" +
                "    , t1.insurance_ceti_no\n" +
                "    , t1.insurance_ceti_type\n" +
                "    , t1.accident_date\n" +
                "    , t1.accident_area\n" +
                "    , t1.accident_area_name--    出现区域名称\n" +
                "    , t1.accident_place\n" +
                "    , t1.accident_process\n" +
                "    , t1.reporter\n" +
                "    , t1.reporter_insured_relation\n" +
                "    , t1.reporter_insured_relation_name\n" +
                "    , t1.tel_no\n" +
                "    , t1.mobile_no\n" +
                "    , t1.claim_amount\n" +
                "    , t1.claim_currency\n" +
                "    , t1.remark\n" +
                "    , t1.handler\n" +
                "    , t1.reported_date\n" +
                "    , t1.death_date\n" +
                "    , t1.close_date\n" +
                "    , t1.creator\n" +
                "    , t1.creator_no\n" +
                "    , t1.gmt_created\n" +
                "    , t1.modifier\n" +
                "    , t1.gmt_modified\n" +
                "    , t1.extra_info\n" +
                "    , t1.extra_info__masterinsuredinfo -- 主被保人\n" +
                "    , t1.extra_info__isneedclaimdivision -- 是否需要理赔分割单类型\n" +
                "    , t1.extra_info__plancodelist -- 滴滴报案时的产品组合\n" +
                "    , t1.extra_info__materialsmssend --  补充材料发送成功标记\n" +
                "    , t1.extra_info__customerno  -- 滴滴报案客户号\n" +
                "    , t1.extra_info__accidentdate --  出险时间\n" +
                "    , t1.extra_info__syslocmobileno --   系统自动定位的手机号\n" +
                "    , t1.extra_info__syncversion --  同步搜索数据版本\n" +
                "    , t1.extra_info__claimreturnchannelkey --  案件回传腾讯商保标记\n" +
                "    , t1.extra_info__importDataSource --  案件导入来源\n" +
                "    , t1.is_deleted\n" +
                "    , t1.allow_dispatch\n" +
                "    , t1.istpadispatch\n" +
                "    , t1.email\n" +
                "    , t1.gender\n" +
                "    , t1.birth_day\n" +
                "    , t1.channel_batch_no\n" +
                "    , t1.channel_report_no\n" +
                "    , t1.adjustment_date\n" +
                "    , t1.tpa_source\n" +
                "    , t1.tpa_source_name\n" +
                "    , t1.master_insured_cert_no\n" +
                "    , t1.master_insured_cert_type\n" +
                "    , t1.is_campaign -- 系统默认为非滴滴案件\n" +
                "    , t1.diagnosis_date\n" +
                "    , t1.is_attachment\n" +
                "    , t1.is_vip\n" +
                "    , t1.elapsed_time\n" +
                "    , t1.disallow_batch_close\n" +
                "    , t1.department\n" +
                "    , t1.has_potential_missing_policy\n" +
                "    , t1.potential_missing_policy\n" +
                "    , t1.lawsuit\n" +
                "    , t1.has_tried_to_dispatch_tpa\n" +
                "    , t1.auto_dispatch_status\n" +
                "    , t1.auto_dispatch_fail_count\n" +
                "    , t1.error_type\n" +
                "    , t1.mdp_hospital_name\n" +
                "    , t1.mdp_hospital_code\n" +
                "    , t1.mdp_invoice_nos\n" +
                "    , t1.order_no\n" +
                "    , t1.claim_date\n" +
                "    , t1.one_notice_date\n" +
                "    , t1.material_complete_date\n" +
                "    , t1.case_mark\n" +
                "    , t1.case_mark_name\n" +
                "    , t1.is_preclaim_adjustment\n" +
                "    , t1.is_internal\n" +
                "    , t1.is_preclaim\n" +
                "    , t1.cancel_operator\n" +
                "    , t1.cancel_review_operator\n" +
                "    , t1.appeal_date\n" +
                "    , t1.appeal_no\n" +
                "    , t1.first_close_date\n" +
                "    , t1.case_tag\n" +
                "    , t1.is_special_drugs\n" +
                "    , t1.source_policy_no -- 保单号\n" +
                "    , t1.source_policy_id -- 保单ID\n" +
                "    , t1.repeat_nos --   注销原因为客户重复报案时选择的重复案件号\n" +
                "    , t1.no_claim_sms -- 是否不需要发送理赔短信 1：不需要， 0/null：需要\n" +
                "    , t1.no_claim_email --   是否不需要发送理赔邮件 1：不需要， 0/null：需要\n" +
                "    , t1.no_customer_visit --    是否不需要客服回访 1：不需要， 0/null：需要\n" +
                "    , t1.risk_remark --  风险备注\n" +
                "    , t1.risk_remark__riskName --  风险备注\n" +
                "    , t1.risk_remark__riskDetail --  风险备注\n" +
                "    , t1.risk_remark__riskResult --  风险备注\n" +
                "    , t1.case_tag_name --案件标签名称\n" +
                "    , t1.catastrophe_code --巨灾代码\n" +
                "    , t1.catastrophe_name --巨灾代码名称\n" +
                "    , t1.elapsed_time_seconds --经过时间(秒)\n" +
                "    , t1.sub_status --理赔子状态\n";
        SqlParser sqlParser = createFlinkParser(sql);

        SqlNode actualNode = sqlParser.parseStmt();
        System.out.println(actualNode);
    }


    @Test
    void testCreateTableLike() throws Exception {
        SqlNode actualNode =
                createFlinkParser(
                        "CREATE TABLE t (\n"
                                + "   a STRING\n"
                                + ")\n"
                                + "LIKE b (\n"
                                + "   EXCLUDING PARTITIONS\n"
                                + "   EXCLUDING CONSTRAINTS\n"
                                + "   EXCLUDING DISTRIBUTION\n"
                                + "   EXCLUDING WATERMARKS\n"
                                + "   OVERWRITING GENERATED\n"
                                + "   OVERWRITING OPTIONS\n"
                                + ")")
                        .parseStmt();

        assertThat(actualNode)
                .satisfies(
                        matching(
                                hasLikeClause(
                                        allOf(
                                                pointsTo("b"),
                                                hasOptions(
                                                        option(
                                                                MergingStrategy.EXCLUDING,
                                                                FeatureOption.PARTITIONS),
                                                        option(
                                                                MergingStrategy.EXCLUDING,
                                                                FeatureOption.CONSTRAINTS),
                                                        option(
                                                                MergingStrategy.EXCLUDING,
                                                                FeatureOption.DISTRIBUTION),
                                                        option(
                                                                MergingStrategy.EXCLUDING,
                                                                FeatureOption.WATERMARKS),
                                                        option(
                                                                MergingStrategy.OVERWRITING,
                                                                FeatureOption.GENERATED),
                                                        option(
                                                                MergingStrategy.OVERWRITING,
                                                                FeatureOption.OPTIONS))))));
    }

    @Test
    void testCreateTableLikeCannotDuplicateOptions() throws Exception {
        ExtendedSqlNode extendedSqlNode =
                (ExtendedSqlNode)
                        createFlinkParser(
                                "CREATE TABLE t (\n"
                                        + "   a STRING\n"
                                        + ")\n"
                                        + "LIKE b (\n"
                                        + "   EXCLUDING PARTITIONS\n"
                                        + "   INCLUDING PARTITIONS\n"
                                        + ")")
                                .parseStmt();

        assertThatThrownBy(extendedSqlNode::validate)
                .isInstanceOf(SqlValidateException.class)
                .hasMessage("Each like option feature can be declared only once.");
    }

    @Test
    void testInvalidOverwritingForPartition() throws Exception {
        ExtendedSqlNode extendedSqlNode =
                (ExtendedSqlNode)
                        createFlinkParser(
                                "CREATE TABLE t (\n"
                                        + "   a STRING\n"
                                        + ")\n"
                                        + "LIKE b (\n"
                                        + "   OVERWRITING PARTITIONS"
                                        + ")")
                                .parseStmt();

        assertThatThrownBy(extendedSqlNode::validate)
                .isInstanceOf(SqlValidateException.class)
                .hasMessage("Illegal merging strategy 'OVERWRITING' for 'PARTITIONS' option.");
    }

    @Test
    void testInvalidOverwritingForAll() throws Exception {
        ExtendedSqlNode extendedSqlNode =
                (ExtendedSqlNode)
                        createFlinkParser(
                                "CREATE TABLE t (\n"
                                        + "   a STRING\n"
                                        + ")\n"
                                        + "LIKE b (\n"
                                        + "   OVERWRITING ALL"
                                        + ")")
                                .parseStmt();

        assertThatThrownBy(extendedSqlNode::validate)
                .isInstanceOf(SqlValidateException.class)
                .hasMessage("Illegal merging strategy 'OVERWRITING' for 'ALL' option.");
    }

    @Test
    void testInvalidOverwritingForDistribution() throws Exception {
        ExtendedSqlNode extendedSqlNode =
                (ExtendedSqlNode)
                        createFlinkParser(
                                "CREATE TABLE t (\n"
                                        + "   a STRING\n"
                                        + ")\n"
                                        + "LIKE b (\n"
                                        + "   OVERWRITING DISTRIBUTION"
                                        + ")")
                                .parseStmt();

        assertThatThrownBy(extendedSqlNode::validate)
                .isInstanceOf(SqlValidateException.class)
                .hasMessageContaining(
                        "Illegal merging strategy 'OVERWRITING' for 'DISTRIBUTION' option.");
    }

    @Test
    void testInvalidOverwritingForConstraints() throws Exception {
        ExtendedSqlNode extendedSqlNode =
                (ExtendedSqlNode)
                        createFlinkParser(
                                "CREATE TABLE t (\n"
                                        + "   a STRING\n"
                                        + ")\n"
                                        + "LIKE b (\n"
                                        + "   OVERWRITING CONSTRAINTS"
                                        + ")")
                                .parseStmt();

        assertThatThrownBy(extendedSqlNode::validate)
                .isInstanceOf(SqlValidateException.class)
                .hasMessageContaining(
                        "Illegal merging strategy 'OVERWRITING' for 'CONSTRAINTS' option.");
    }

    @Test
    void testInvalidNoOptions() {
        SqlParser parser =
                createFlinkParser("CREATE TABLE t (\n" + "   a STRING\n" + ")\n" + "LIKE b ()");
        assertThatThrownBy(parser::parseStmt)
                .isInstanceOf(SqlParseException.class)
                .hasMessageStartingWith(
                        "Encountered \")\" at line 4, column 9.\n"
                                + "Was expecting one of:\n"
                                + "    \"EXCLUDING\" ...\n"
                                + "    \"INCLUDING\" ...\n"
                                + "    \"OVERWRITING\" ...");
    }

    @Test
    void testInvalidNoSourceTable() {
        SqlParser parser =
                createFlinkParser(
                        "CREATE TABLE t (\n"
                                + "   a STRING\n"
                                + ")\n"
                                + "LIKE ("
                                + "   INCLUDING ALL"
                                + ")");

        assertThatThrownBy(parser::parseStmt)
                .isInstanceOf(SqlParseException.class)
                .hasMessageStartingWith(
                        "Encountered \"(\" at line 4, column 6.\n"
                                + "Was expecting one of:\n"
                                + "    <BRACKET_QUOTED_IDENTIFIER> ...\n"
                                + "    <QUOTED_IDENTIFIER> ...\n"
                                + "    <BACK_QUOTED_IDENTIFIER> ...\n"
                                + "    <BIG_QUERY_BACK_QUOTED_IDENTIFIER> ...\n"
                                + "    <HYPHENATED_IDENTIFIER> ...\n"
                                + "    <IDENTIFIER> ...\n"
                                + "    <UNICODE_QUOTED_IDENTIFIER> ...\n");
    }

    public static SqlTableLikeOption option(
            MergingStrategy mergingStrategy, FeatureOption featureOption) {
        return new SqlTableLikeOption(mergingStrategy, featureOption);
    }

    private static Matcher<SqlTableLike> hasOptions(SqlTableLikeOption... optionMatchers) {
        return new FeatureMatcher<SqlTableLike, List<SqlTableLikeOption>>(
                equalTo(Arrays.asList(optionMatchers)), "like options equal to", "like options") {
            @Override
            protected List<SqlTableLikeOption> featureValueOf(SqlTableLike actual) {
                return actual.getOptions();
            }
        };
    }

    private static Matcher<SqlTableLike> hasNoOptions() {
        return new FeatureMatcher<SqlTableLike, List<SqlTableLikeOption>>(
                empty(), "like options are empty", "like options") {
            @Override
            protected List<SqlTableLikeOption> featureValueOf(SqlTableLike actual) {
                return actual.getOptions();
            }
        };
    }

    private static Matcher<SqlTableLike> pointsTo(String... table) {
        return new FeatureMatcher<SqlTableLike, String[]>(
                equalTo(table), "source table identifier pointing to", "source table identifier") {

            @Override
            protected String[] featureValueOf(SqlTableLike actual) {
                return actual.getSourceTable().names.toArray(new String[0]);
            }
        };
    }

    private static Matcher<SqlNode> hasLikeClause(Matcher<SqlTableLike> likeMatcher) {
        return new FeatureMatcher<SqlNode, SqlTableLike>(
                likeMatcher, "create table statement has like clause", "like clause") {

            @Override
            protected SqlTableLike featureValueOf(SqlNode actual) {
                if (!(actual instanceof SqlCreateTableLike)) {
                    throw new AssertionError("Node is not a CREATE TABLE stmt.");
                }
                return ((SqlCreateTableLike) actual).getTableLike();
            }
        };
    }

    private SqlParser createFlinkParser(String expr) {
        SqlParser.Config parserConfig =
                SqlParser.config()
                        .withParserFactory(FlinkSqlParserImpl.FACTORY)
                        .withLex(Lex.JAVA)
                        .withIdentifierMaxLength(256);

        return SqlParser.create(expr, parserConfig);
    }
}
