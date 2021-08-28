/*
 * Licensed to the Ted Dunning under one or more contributor license
 * agreements.  See the NOTICE file that may be
 * distributed with this work for additional information
 * regarding copyright ownership.  Ted Dunning licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.mapr.synth.samplers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Random;

/**
 * Sample a list of headers in the style of those that might accompany a web request
 */
public class HeaderSampler extends FieldSampler {

    private Template template;
    private String prolog;

    private enum Type {
        NORMAL,
        ABABIL,
        MAL1,
        MAL2,
        MAL3
    }

    private Type headerType = Type.NORMAL;

    private final BrowserSampler browser = new BrowserSampler();
    private final Map<String, StringSampler> headers = ImmutableMap.<String, StringSampler>builder()
            .put("chrome", new StringSampler("user-agents/chrome"))
            .put("firefox", new StringSampler("user-agents/firefox"))
            .put("ie", new StringSampler("user-agents/ie"))
            .put("mobile", new StringSampler("user-agents/mobile"))
            .put("opera", new StringSampler("user-agents/opera"))
            .put("safari", new StringSampler("user-agents/safari"))
            .build();
    private final StringSampler language = new LanguageSampler();

    private final Random gen = new Random();

    public void setType(String headerType) throws IOException {
        this.headerType = Type.valueOf(headerType.toUpperCase());
        setupTemplate();
    }

    @SuppressWarnings("unused")
    public void setProlog(String prolog) {
        this.prolog = prolog;
    }

    // picks which template to use based on header type
    private void setupTemplate() throws IOException {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_21);
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        cfg.setTemplateLoader(new ClassTemplateLoader(getClass(), "/web-headers"));
        String templateName = "header";
        switch (headerType) {
            case MAL3:
                templateName = "mal3";
                break;
            case ABABIL:
                templateName = "ababil";
                break;
            default:
                break;
        }
        template = cfg.getTemplate(templateName);
    }

    // these methods sample the pieces of the headers
    String encoding() {
        switch (headerType) {
            case MAL1:
                return "identity";
            case MAL2:
                return "             ";
            default:
                switch (gen.nextInt(3)) {
                    case 0:
                        return "gzip";
                    case 1:
                        return "deflate";
                    case 2:
                    default:
                        return "gzip, deflate";
                }
        }
    }

    String userAgent() {
        if (headerType == Type.ABABIL) {
            return "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;)";
        }
        String br = browser.sample().asText();
        StringSampler s = headers.get(br.toLowerCase());
        if (s == null) {
            throw new IllegalStateException("Illegal browser");
        }
        return s.sample().asText();
    }

    String language() {
        String lang = language.sample().asText();
        if (headerType == Type.ABABIL) {
            return "fr";
        } else {
            return lang + "-" + lang.toUpperCase() + "," + lang + ";q=0.5";
        }
    }

    String url(boolean isImage) {
        return String.format("http://foo.bar.com/%06d/%06x%s",
                gen.nextInt(1_000_000), gen.nextInt(0x1_000_000),
                isImage ? ".jpg" : ".html");
    }

    String accept(boolean isImage) {
        if (isImage) {
            return "image/png,image/*;q=0.8,*/*;q=0.5";
        } else {
            return "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
        }
    }

    public HeaderSampler() throws IOException {
        setupTemplate();
    }

    @Override
    public JsonNode sample() {
        boolean isImage = gen.nextDouble() < 0.3;
        Map<String, String> params = ImmutableMap.<String, String>builder()
                .put("url", url(isImage))
                .put("host", String.format("x%03d.foo.com", gen.nextInt(5)))
                .put("accept", accept(isImage))
                .put("userAgent", userAgent())
                .put("language", language())
                .put("encoding", encoding())
                .put("referer", url(false))
                .build();
        StringWriter out = new StringWriter();
        if (prolog != null) {
            out.append(prolog);
        }
        try {
            template.process(params, out);
        } catch (TemplateException e) {
            throw new RuntimeException("Error expanding template", e);
        } catch (IOException e) {
            throw new RuntimeException("Error loading template", e);
        }
        return new TextNode(out.toString());
    }
}
