package com.web;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.etl.es.EsMappingEtl;
import com.service.EsQueryService;
import com.support.EsTag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

@Controller
public class EsQueryController {

    @Autowired
    EsQueryService service;

    @RequestMapping("gen")
    public  void genAndDown(@RequestBody String data, HttpServletResponse response) {
        JSONObject object = JSON.parseObject(data);
        JSONArray selectedTags = object.getJSONArray("selectedTags");
        List<EsTag> list = selectedTags.toJavaList(EsTag.class);
        List<EsMappingEtl.MemberTag> tags = service.buildQuery(list);

//        List<EsMappingEtl.MemberTag> tags = new ArrayList<>();
//        EsMappingEtl.MemberTag memberTag = new EsMappingEtl.MemberTag();
//        memberTag.setMemberId("123456");
//        memberTag.setPhone("11111111111");
//        tags.add(memberTag);

        String content = toContent(tags);

        String filename = "member.txt";
        response.setContentType("text/plain");

        try {
            response.setHeader("content-Disposition", "attachment; filename=" + URLEncoder.encode(filename,"UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        ServletOutputStream sos = null;
        BufferedOutputStream bos = null;

        try {
            sos = response.getOutputStream();
            bos = new BufferedOutputStream(sos);
            bos.write(content.getBytes("UTF-8"));
            bos.flush();
            bos.close();
            sos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private String toContent(List<EsMappingEtl.MemberTag> tags) {
        StringBuilder sb = new StringBuilder();
        for (EsMappingEtl.MemberTag tag : tags) {
            sb.append("["  + tag.getMemberId() + "," + tag.getPhone() + "]\r\n");
        }
        return sb.toString();
    }
}
