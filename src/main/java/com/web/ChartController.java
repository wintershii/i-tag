package com.web;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * @author bywind
 */
@Controller
@Slf4j
public class ChartController {
    //springboot web

    //用户点选标签集合 -->  DSL 标签语句
    @RequestMapping("/tags")
    public String chart(){
        return "tags";
    }

    @RequestMapping("/")
    public String index() {
        return "index";
    }
}
