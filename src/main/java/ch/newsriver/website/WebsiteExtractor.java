package ch.newsriver.website;


import ch.newsriver.website.alexa.AlexaClient;
import ch.newsriver.website.alexa.AlexaSiteInfo;
import ch.newsriver.util.HTMLUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.time.LocalDate.now;

/**
 * Created by eliapalme on 03/04/16.
 */
public class WebsiteExtractor {

    private static final Logger logger = LogManager.getLogger(WebsiteExtractor.class);


    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final Map<String, String> countries = new HashMap<>();
    private static final Map<String, List<Double>> geoCountriesCapitals = new HashMap<>();
    static PublicSuffixList suffixList;
    static PublicSuffixMatcher matcher;
    static {


        for (String iso : Locale.getISOCountries()) {
            Locale l = new Locale("", iso);
            countries.put(l.getDisplayCountry().toLowerCase(), iso);
        }

        String listname="suffixlist.txt";
        try (InputStream inputStream = WebsiteExtractor.class.getClassLoader().getResourceAsStream(listname)){

            //suffix list is optained from https://publicsuffix.org
            if (inputStream != null) {
                suffixList = new PublicSuffixListParser().parse(new InputStreamReader(inputStream, Consts.UTF_8));
            }
        } catch (Exception e) {
            logger.fatal("Unable to load public suffix List", e);
        }

        matcher = new PublicSuffixMatcher(suffixList.getRules(), suffixList.getExceptions());

        try (InputStream inputStream = WebsiteExtractor.class.getClassLoader().getResourceAsStream("capitals.geo.json")){



            //geogeson obtained by https://github.com/MapSurferNET/MapSurfer.NET-Examples/tree/master/MapData/OpenStreetMap/World
            //
            //other sources
            //geogeson obtained by https://github.com/johan/world.geo.json
            //https://github.com/datasets/geo-countries
            if (inputStream != null) {
                ObjectMapper mapper = new ObjectMapper();
               Iterator<JsonNode> iterator = mapper.readTree(inputStream).get("features").elements();
                while(iterator.hasNext()){
                    JsonNode node =iterator.next();
                    JsonNode properties = node.get("properties");
                    JsonNode geometry = node.get("geometry");
                    if(properties!=null && geometry!=null && properties.has("is_in:country_code") && geometry.has("coordinates")) {
                        geoCountriesCapitals.put(properties.get("is_in:country_code").textValue(), mapper.convertValue(geometry.get("coordinates"), List.class));
                    }
                }
            }
        } catch (Exception e) {
            logger.fatal("Unable to load public suffix List", e);
        }

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

        if(alexaSiteInfo!=null) {
            String country = null;
            try {
                country = URLDecoder.decode(alexaSiteInfo.getCountry(), "utf-8").toLowerCase();
            }catch (UnsupportedEncodingException e){
                logger.fatal(e);
            }
            if(country!=null){
                country = countries.get(country);
            }


            String language = alexaSiteInfo.getLanguage();
            if(language.equalsIgnoreCase("x-unknown")){
                language = null;
            }else if(language.contains("-")){
                String[] parts = language.split("-");
                language = parts[0];
                if(country == null){
                    country=parts[1];
                }
            }
            Locale.Builder builder = new Locale.Builder();
            if(language!=null){
                builder.setLanguage(language);
            }
            if(country!=null){
                builder.setRegion(country);
            }
            Locale locale = builder.build();
            if (locale == null) {
                logger.error("Unable to retreive locale");
                return null;
            }else{
                webSite.setCountryName(locale.getDisplayCountry());
                webSite.setCountryCode(locale.getISO3Country());
                webSite.getLanguages().add(locale.getISO3Language());
            }

            if(locale.getCountry()!=null && !locale.getCountry().isEmpty()){
                webSite.setGeoLocation(geoCountriesCapitals.get(locale.getCountry()));
            }

            webSite.setRankingGlobal(alexaSiteInfo.getGlobalRank());
            for(String countryCode : alexaSiteInfo.getCountryRank().keySet()){
                if(locale.getCountry().equalsIgnoreCase(countryCode)){
                    webSite.setRankingCountry(alexaSiteInfo.getCountryRank().get(countryCode));
                    break;
                }
            }
            try {
                webSite.setName(URLDecoder.decode(alexaSiteInfo.getTitle(), "utf-8"));
            }catch (UnsupportedEncodingException e){

                webSite.setName(alexaSiteInfo.getTitle());
            }
            try {
                webSite.setDescription(URLDecoder.decode(alexaSiteInfo.getDescription(), "utf-8"));
            }catch (UnsupportedEncodingException e){
                webSite.setDescription(alexaSiteInfo.getDescription());
            }
        }


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
            webSite.setCanonicalURL(getUrl(webSite));
            webSite.setDomainName(matcher.getDomainRoot(webSite.getCanonicalURL()));


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
        if(webSite.getPort()>0 && ((webSite.isSsl() && webSite.getPort()!=443) || (!webSite.isSsl() && webSite.getPort()!=80))){
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
            url = absoluteURL(link.attr("href").trim(), webSite.getCanonicalURL());
        }
        return url;
    }

    public String getCanonicalURL(Element link, WebSite webSite){
        String url=null;
        if(link.hasAttr("rel") && link.attr("rel").equalsIgnoreCase("canonical") && link.hasAttr("href")){
            url = absoluteURL(link.attr("href").trim(), webSite.getCanonicalURL());
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
