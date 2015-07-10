package com.meituan.storm.parser;

import com.meituan.storm.common.AbstractLogParser;
import com.meituan.storm.common.metrics.api.MTMeanMetric;
import org.json.JSONObject;
import org.json.JSONException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BLogParser extends AbstractLogParser implements Serializable {
	private static final long serialVersionUID = -3715799651295194837L;
	
	private static final Logger LOG = LoggerFactory.getLogger(BLogParser.class);

	private static final String SHOP_ID = ".*/shop/(\\d+)";
	private static final String DEAL_ID = ".*/deal/(\\d+)\\.html";
	private static final String DEAL_ID2 = ".*/deal/(\\d+)";
	private static final String BUY_ID = ".*/deal/buy/(\\d+)";
	private static final String CARD_ID = ".*/card/default/(\\d+)";
	private static final String SEARCH = ".*/s/\\?w=([^&]+)";

	private static final String CATEGORY = "^/category/([\\w]+)";
	private static final String SEARCH_CATEGORY = "^/s/[^/]+/([\\w]+)";
	private static final String HOTEL_SEARCH_CATEGORY = "^/hotel/search/([\\w]+)";
	private static final String DIANYING_CATEGORY = "^/dianying(.*)";

	private static final String GEO = "^/category/[^/]+/([\\w]+)";
	private static final String SEARCH_GEO = "^/s/[^/]+/[^/]+/([\\w]+)";
	private static final String HOTEL_SEARCH_GEO = "^/hotel/search/[^/]+/[^/]+/([\\w]+)";

	// 需要城市id的feature
	private HashSet<String> needCityFeature;

	private DateFormat timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public BLogParser() {
		needCityFeature = new HashSet<String>();
		needCityFeature.add("pc_search");
		needCityFeature.add("pc_movie");
	}

	private String filterType = null;

	private String matchPattern(String str, String p) {
		String result = null;

		Pattern pattern = Pattern.compile(p);
		Matcher matcher = pattern.matcher(str);
		if(matcher.find()) {
			result = matcher.group(1);
		}
		return result;
	}

	private String getShopId(String url) {
		return matchPattern(url, SHOP_ID);
	}

	private String getDealId(String url) {
		if (url == null) {
			return null;
		}
		String dealId = matchPattern(url, DEAL_ID);

		if(dealId == null) {
			dealId = matchPattern(url, DEAL_ID2);
		}

		return dealId;
	}

	private String getBuyDealId(String url) {
		String dealId = matchPattern(url, BUY_ID);

		if(dealId == null) {
			dealId = matchPattern(url, CARD_ID);
		}

		return dealId;
	}

	private String hexToString(String source) {
		String sourceArr[] = source.split("\\\\"); // 分割拿到形如 xE9 的16进制数据
		byte[] byteArr = new byte[sourceArr.length - 1];
		for (int i = 1; i < sourceArr.length; i++) {
			Integer hexInt = Integer.decode("0" + sourceArr[i]);
			byteArr[i - 1] = hexInt.byteValue();
		}
		try {
			return new String(byteArr, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return source;
		}
	}

	private String getQueryTerm(String url) {
		String term = matchPattern(url, SEARCH);
		if (term != null) {
			try {
				term = URLDecoder.decode(term, "utf-8").trim();
			} catch (Exception e) {
				return null;
			}
		}
		return term;
	}

	private String getCategory(String url) {
		String cate = matchPattern(url, CATEGORY);

		if(cate == null) {
			cate = matchPattern(url, SEARCH_CATEGORY);
		}
		if(cate == null) {
			cate = matchPattern(url, HOTEL_SEARCH_CATEGORY);
		}
		if(cate == null && matchPattern(url, DIANYING_CATEGORY) != null) {
			cate = "dianying";
		}

		if ("all".equals(cate)) {
			cate = null;
		}

		return cate;
	}

	private String getGeo(String url) {
		String geo = matchPattern(url, GEO);

		if(geo == null) {
			geo = matchPattern(url, SEARCH_GEO);
		}
		if(geo == null) {
			geo = matchPattern(url, HOTEL_SEARCH_GEO);
		}

		if ("all".equals(geo)) {
			geo = null;
		}

		return geo;
	}

	public String parseUrl(String url) {
		filterType = null;

		String obj = getDealId(url);
		if(obj != null) {
			filterType = "pc_view";
			return obj;
		}

		obj = getQueryTerm(url);
		if (obj != null) {
			filterType = "pc_search";
			return obj;
		}

		obj = getCategory(url);
		if (obj != null) {
			filterType = "pc_cate_select";
			return obj;
		}

		obj = getGeo(url);
		if (obj != null) {
			filterType = "pc_geo_select";
			return obj;
		}

		obj = getBuyDealId(url);
		if (obj != null) {
			filterType = "pc_order";
			return obj;
		}

		obj = getShopId(url);
		if (obj != null) {
			filterType = "pc_shop";
			return obj;
		}

		return obj;
	}

	public String getField(JSONObject logJO, String fieldName) {
        try {
            return logJO.getString(fieldName);
        } catch (JSONException x) {
            return null;
        }
    }

	public void addOne(HashMap<String, Integer> counter, String key) {
		Integer val = counter.get(key);
		if (val == null) {
			val = 0;
		}
		counter.put(key, ++val);
	}

	public void incCounter(JSONObject logJO, HashMap<String, Integer> counter) {
		String referer = getField(logJO, "urlinfo_r");
		URL referer_url = null;
		if (referer != null) {
			referer = referer.trim();
			if (!referer.startsWith("http://")) {
				referer = "http://" + referer;
			}
			try {
				referer_url = new URL(referer);
			} catch (MalformedURLException e) {
				LOG.info(e.getMessage());
			}
		}

		String trace_b = getField(logJO, "trace_B");
		String trace_c = getField(logJO, "trace_C");
		String trace_d = getField(logJO, "trace_D");
		String trace_g = getField(logJO, "trace_G");

		// 个性化商圈
		if (filterType != null && filterType.contains("select")) {
			if (referer_url != null && referer_url.getPath().equals("/")) {
				if (trace_b != null && trace_b.equals("shoppingmall")
						&& trace_c != null) {
					addOne(counter, "category.shoppingmall." + trace_c);
				}
			}
		}

		// 详情页来源
		String referer_dealid = getDealId(referer);
		if (filterType != null && filterType.contains("view")) {
		 	addOne(counter, "deal.view");
			if (referer_url != null) {
				if (referer_url.getHost().contains("meituan")) {
					if (referer_url.getPath().equals("/")) {
						addOne(counter, "deal.index");
					} else if (referer_dealid != null) {
						addOne(counter, "deal.deal");
					} else if (referer_url.getPath().contains("category")) {
						addOne(counter, "deal.category");
					} else if (getQueryTerm(referer) != null) {
						addOne(counter, "deal.search");
					}
				} else {
					addOne(counter, "deal.thirdparty");
				}
			}
		}

		// 推荐效果跟踪
		if (trace_b != null && referer_dealid != null) {
			if (trace_b.equals("dealrightside") || trace_b.equals("filterrightside")) {
				addOne(counter, "deal.rec.right");
				if (trace_c != null) {
					addOne(counter, "deal.rec.S." + trace_c);
				}
				if (trace_d != null) {
					addOne(counter, "deal.rec.M." + trace_d);
				}
				if (trace_g != null) { // position
					addOne(counter, "deal.rec.P." + trace_g);
				}
			}

			if (trace_b.equals("dealbottom")) {
				addOne(counter, "deal.rec.bottom");
				if (trace_c != null) {
					addOne(counter, "deal.rec.Sb." + trace_c);
				}
				if (trace_d != null) {
					addOne(counter, "deal.rec.Mb." + trace_d);
				}
				if (trace_g != null) { // position
					addOne(counter, "deal.rec.Pb." + trace_g);
				}
			}
		}
	}

    @Override
	public String parse(String log, MTMeanMetric rMetric) {
		JSONObject logJO = null;
		try {
			logJO = new JSONObject(log);
		}catch(JSONException jsone) {
			LOG.error(jsone.toString());
			return null;
		}

		JSONObject result = new JSONObject();
		String uuid = getField(logJO, "GA_UUID");
		if (uuid == null) {
			return null;
		}

		String cityId = getField(logJO, "ci");
		String userId = getField(logJO, "u");

		String srvtime = getField(logJO, "srvtime");
		Long time = null;
		if (srvtime != null) {
			try {
				time = timeFormatter.parse(srvtime).getTime();
				long end = System.currentTimeMillis();
				rMetric.update((end - time) / 1000);
			} catch (ParseException e) {
			}
		}
		if (time == null) {
			time = (new Date()).getTime();
		}

		String feature = null;
		String url = getField(logJO, "referer_url");
		if (url != null) {
			feature = parseUrl(url);
			// filter and search and movie is per city, jiudian is all city
			if (feature != null && (needCityFeature.contains(filterType) || filterType.contains("select"))
					&& cityId != null && !feature.contains("jiudian")) {
				feature += "_" + cityId;
			}
		}

		HashMap<String, Integer> counter = new HashMap();
		incCounter(logJO, counter);

		try {
			result.put("uuid", uuid);
			result.put("time", time);
			result.put("counter", counter);

			if (filterType != null) {
				result.put("type", filterType);
				result.put("feature", feature);
			}

			if (userId != null) {
				result.put("userId", userId);
			}
		}catch(JSONException x) {
			LOG.error(x.toString());
		}

		return result.toString();
	}
}	
