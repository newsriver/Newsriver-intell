package ch.newsriver.data.website;


import ch.newsriver.data.html.HTML;
import ch.newsriver.data.website.alexa.AlexaClient;
import ch.newsriver.data.website.alexa.AlexaSiteInfo;
import ch.newsriver.util.HTMLUtils;
import com.google.common.base.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Consts;
import org.apache.http.conn.util.PublicSuffixList;
import org.apache.http.conn.util.PublicSuffixListParser;
import org.apache.http.conn.util.PublicSuffixMatcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;

import static java.time.LocalDate.now;

/**
 * Created by eliapalme on 03/04/16.
 */
public class WebsiteExtractor {

    private static final Logger logger = LogManager.getLogger(WebsiteExtractor.class);


    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    static PublicSuffixList suffixList;
    static PublicSuffixMatcher matcher;
    static {

        String listname="/suffixlist.txt";
        try (InputStream inputStream = WebsiteExtractor.class.getClassLoader().getResourceAsStream(listname)){

            //suffix list is optained from https://publicsuffix.org
            if (inputStream != null) {
                suffixList = new PublicSuffixListParser().parse(new InputStreamReader(inputStream, Consts.UTF_8));
            }
        } catch (Exception e) {
            logger.fatal("Unable to load public suffix List", e);
        }

        matcher = new PublicSuffixMatcher(suffixList.getRules(), suffixList.getExceptions());
    }



    public WebSite extract(String url){

        WebSite webSite = new WebSite();
        try {
            setWebsiteConicalURL(url,webSite);
        }catch (URISyntaxException ex){
            logger.error("Invalid URL syntax",ex);
            return  null;
        }


        //Get Alexa information
        AlexaSiteInfo alexaSiteInfo = getAlexaSiteInfo(webSite.getCanonicalURL());


        Locale locale = new Locale.Builder().setLanguage(alexaSiteInfo.getLanguage()).setRegion(alexaSiteInfo.getCountry()).build();
        if(locale == null){
            logger.error("Unable to retreive locale");
            return  null;
        }
        webSite.setLocale(locale);
        webSite.setRankingGlobal(alexaSiteInfo.getGlobalRank());
        for(String country : alexaSiteInfo.getCountryRank().keySet()){
            if(locale.getISO3Country().equalsIgnoreCase(country)){
                webSite.setRankingCountry(alexaSiteInfo.getCountryRank().get(country));
                break;
            }
        }
        webSite.setName(alexaSiteInfo.getTitle());
        webSite.setDescription(alexaSiteInfo.getDescription());

        Elements linkTags = getLinkTags(webSite.getCanonicalURL());
        if (linkTags !=null && !linkTags.isEmpty()){
            for(Element link : linkTags){

                String icon =  getIconURL(link, webSite);
                if(icon!=null){
                    webSite.setIconURL(icon);
                    continue;
                }

                String feed =  getFeedURL(link, webSite);
                if(feed!=null){
                    webSite.getFeeds().add(feed);
                    continue;
                }

                String canonical =  getCanonicalURL(link, webSite);
                if(canonical!=null){
                    if(!canonical.equals(webSite.getCanonicalURL())) {
                        try {
                            String alternative = webSite.getCanonicalURL();
                            setWebsiteConicalURL(canonical,webSite);
                            webSite.getAlternativeURLs().add(alternative);
                        }catch (URISyntaxException ex){
                            logger.error("Invalid canonical URL syntax",ex);
                            return  null;
                        }
                    }
                }
            }

        }


        webSite.setLastUpdate(simpleDateFormat.format(new Date()));
        return webSite;
    }

    private void setWebsiteConicalURL(String url, WebSite webSite ) throws  URISyntaxException{
            URI uri = new URI(url);
            webSite.setHostName(uri.getHost().toLowerCase());
            webSite.setPort(uri.getPort());
            if(uri.getScheme().toLowerCase().startsWith("https")){
                webSite.setSsl(true);
            }
            webSite.setDomainName(getRootDomain(url).toLowerCase());
            webSite.setCanonicalURL(getUrl(webSite));

    }


    public String getRootDomain(String url){
        return matcher.getDomainRoot(url);
    }

    public AlexaSiteInfo getAlexaSiteInfo(String url){
        AlexaClient alexaClient = new AlexaClient();
        return alexaClient.getSiteInfo(url);
    }

    public String getUrl(WebSite webSite){
        StringBuilder url = new StringBuilder();
        if(webSite.isSsl()){
            url.append("https://");
        }else{
            url.append("http://");
        }
        url.append(webSite.getHostName());
        if((webSite.isSsl() && webSite.getPort()!=443) || (!webSite.isSsl() && webSite.getPort()!=80)){
            url.append(":");
            url.append(webSite.getPort());
        }
        return url.toString();
    }

    public Elements getLinkTags(String url) {

        Elements links = null;
        try {
            String htmlStr =  HTMLUtils.getHTML(url, false);

            Document doc = Jsoup.parse(htmlStr);
            links = doc.getElementsByTag("link");


        } catch (IOException e) {
            logger.error("Error fetching html", e);
        } catch (Exception e) {
            logger.error("Error fatching html", e);
        }
        return links;
    }



    public String getIconURL(Element link, WebSite webSite){
        String url=null;
        if(link.hasAttr("rel") && link.attr("rel").equalsIgnoreCase("icon") && link.hasAttr("href")){
            url = absoluteURL(link.attr("href"), webSite.getCanonicalURL());
        }
        return url;
    }

    public String getFeedURL(Element link, WebSite webSite){
        String url=null;
        if(link.hasAttr("rel") && link.attr("rel").equalsIgnoreCase("alternate") && link.hasAttr("type") && link.attr("type").equalsIgnoreCase("application/rss+xml") && link.hasAttr("href")){
            url = absoluteURL(link.attr("href"), webSite.getCanonicalURL());
        }
        return url;
    }

    public String getCanonicalURL(Element link, WebSite webSite){
        String url=null;
        if(link.hasAttr("rel") && link.attr("rel").equalsIgnoreCase("canonical") && link.hasAttr("href")){
            url = absoluteURL(link.attr("href"), webSite.getCanonicalURL());
        }
        return url;
    }


    private String absoluteURL(String url, String baseURL){
        try{
        URI uri = new URI(url);
        if(uri.isAbsolute()){
            return uri.toString();
        }else{
            URI base = new URI(baseURL);
            return base.resolve(uri).toString();
        }
        }catch (URISyntaxException ex){
            logger.error("Invalid url",ex);
            return  null;
        }
    }

}
