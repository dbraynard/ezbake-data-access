/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.data.elastic;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.time.DateUtils;
import org.apache.thrift.TException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceFilterBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import ezbake.base.thrift.AdvancedMarkings;
import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.data.base.thrift.PurgeResult;
import ezbake.data.elastic.common.ElasticUtils;
import ezbake.data.elastic.test.EzElasticTestUtils;
import ezbake.data.elastic.test.Location;
import ezbake.data.elastic.test.PlaceOfInterest;
import ezbake.data.elastic.thrift.DateField;
import ezbake.data.elastic.thrift.DateHistogramFacet;
import ezbake.data.elastic.thrift.DateInterval;
import ezbake.data.elastic.thrift.DateIntervalType;
import ezbake.data.elastic.thrift.Document;
import ezbake.data.elastic.thrift.DocumentIdentifier;
import ezbake.data.elastic.thrift.Facet;
import ezbake.data.elastic.thrift.FacetRequest;
import ezbake.data.elastic.thrift.FacetResult;
import ezbake.data.elastic.thrift.FieldSort;
import ezbake.data.elastic.thrift.HighlightRequest;
import ezbake.data.elastic.thrift.HighlightedField;
import ezbake.data.elastic.thrift.IndexResponse;
import ezbake.data.elastic.thrift.MalformedQueryException;
import ezbake.data.elastic.thrift.PercolateQuery;
import ezbake.data.elastic.thrift.PercolateRequest;
import ezbake.data.elastic.thrift.RangeFacetEntry;
import ezbake.data.elastic.thrift.ScriptParam;
import ezbake.data.elastic.thrift.SearchResult;
import ezbake.data.elastic.thrift.SortCriteria;
import ezbake.data.elastic.thrift.SortOrder;
import ezbake.data.elastic.thrift.TermsFacet;
import ezbake.data.elastic.thrift.TermsFacetEntry;
import ezbake.data.elastic.thrift.TermsScriptFacet;
import ezbake.data.elastic.thrift.TermsStatsFacet;
import ezbake.data.elastic.thrift.TermsStatsFacetResultEntry;
import ezbake.data.elastic.thrift.TermsStatsValue;
import ezbake.data.elastic.thrift.UpdateOptions;
import ezbake.data.elastic.thrift.UpdateScript;
import ezbake.data.elastic.thrift.ValueScript;
import ezbake.data.test.TestUtils;

@SuppressWarnings({"StaticNonFinalField"})
public final class ElasticClientTest {
    private static final String TEST_TYPE = "sample";
    private static final String APPLICATION_NAME = "elastic-client-unit-tests";
    private static final EzSecurityToken COMMON_USER_TOKEN = TestUtils.createTestToken("A");

    private static final Gson gson = new GsonBuilder().setDateFormat(EzElasticTestUtils.DATE_FORMAT).create();

    private static EsSetup esSetup;

    private PlaceOfInterest jeffersonMemorial;
    private Document jeffersonMemorialDoc;
    private Document jeffersonMemorialDocWithVis;

    private PlaceOfInterest whiteHouse;
    private Document whiteHouseDoc;
    private Document whiteHouseDocWithVis;

    private PlaceOfInterest columbia;
    private Document columbiaDoc;
    private Document columbiaDocWithVis;

    private PlaceOfInterest lincolnMemorial;
    private Document lincolnMemorialDoc;
    private Document lincolnMemorialDocWithVis;

    private ElasticClient client;

    @BeforeClass
    public static void setupClass() throws Exception {
        final Settings settings = ImmutableSettings.settingsBuilder().put("script.disable_dynamic", false)
                .put("script.native.visibility.type", "ezbake.data.elastic.security.EzSecurityScriptFactory").build();

        esSetup = new EsSetup(settings);
        esSetup.execute(EsSetup.deleteAll());

        if (esSetup.client() == null) {
            throw new Exception("Could not start EsSetup node!");
        }
    }

    @AfterClass
    public static void teardownClass() {
        esSetup.terminate();
    }

    @Before
    public void setup() throws IOException {
        esSetup.execute(EsSetup.deleteAll());

        client = new ElasticClient(esSetup.client(), APPLICATION_NAME, true, 1);
        setMappingForTest();

        final Calendar visit = new GregorianCalendar();
        Visibility aVis = new Visibility().setFormalVisibility("A");

        // Jefferson Memorial
        jeffersonMemorial = new PlaceOfInterest();
        jeffersonMemorial.setTitle("Jefferson Memorial");
        jeffersonMemorial.setComments(
                "The Thomas Jefferson Memorial is a presidential monument in Washington, D.C., dedicated to Thomas "
                        + "Jefferson, (1743–1826), one of the most important of the American \"Founding Fathers\" as "
                        + "the main drafter and writer of the \"Declaration of Independence\", member of the "
                        + "Continental Congress, Governor of the newly independent Commonwealth of Virginia, American"
                        + " minister to King Louis XVI and the Kingdom of France, first U.S. Secretary of State under"
                        + " the first President George Washington, the second Vice President of the United States "
                        + "under second President John Adams, and also the third President of the United States, "
                        + "(1801–1809).");
        jeffersonMemorial.setLocation(new Location(38.889468, -77.03524));
        jeffersonMemorial.setRating(92);
        jeffersonMemorial.setVisit(visit.getTime());
        jeffersonMemorial.setTags(Sets.newHashSet("jefferson", "memorial", "dc"));
        String jeffersonMemorialJson = gson.toJson(jeffersonMemorial);
        jeffersonMemorialDoc = EzElasticTestUtils.generateDocument(TEST_TYPE, jeffersonMemorialJson, aVis);
        jeffersonMemorialDocWithVis = EzElasticTestUtils
                .generateDocument(TEST_TYPE, jeffersonMemorialJson, new Visibility().setFormalVisibility("A&B&(C|D)"));

        // Columbia
        columbia = new PlaceOfInterest();
        columbia.setTitle("Columbia");
        columbia.setComments(
                "Columbia is a planned community comprising 10 self-contained villages, located in Howard "
                        + "County, Maryland—the second wealthiest county in the United States, according to "
                        + "2013 U.S. Census Bureau figures.");
        columbia.setLocation(new Location(39.182786, -76.808853));
        columbia.setRating(92);
        visit.add(Calendar.HOUR, -5);
        columbia.setVisit(visit.getTime());
        columbia.setTags(Sets.newHashSet("columbia", "community", "md"));
        String columbiaJson = gson.toJson(columbia);
        columbiaDoc = EzElasticTestUtils.generateDocument(TEST_TYPE, columbiaJson, aVis);
        columbiaDocWithVis = EzElasticTestUtils
                .generateDocument(TEST_TYPE, columbiaJson, new Visibility().setFormalVisibility("A&B&E"));

        //Lincoln Memorial
        lincolnMemorial = new PlaceOfInterest();
        lincolnMemorial.setTitle("Lincoln Memorial");
        lincolnMemorial.setComments(
                "The Lincoln Memorial is an American national monument built to honor the 16th President of the "
                        + "United States, Abraham Lincoln. Made of white stone.");
        lincolnMemorial.setLocation(new Location(38.888481, -77.051518));
        lincolnMemorial.setRating(2);
        visit.add(Calendar.HOUR, -2);
        lincolnMemorial.setVisit(visit.getTime());
        lincolnMemorial.setTags(Sets.newHashSet("lincoln", "memorial", "dc"));
        String lincolnMemorialJson = gson.toJson(lincolnMemorial);
        lincolnMemorialDoc = EzElasticTestUtils.generateDocument(TEST_TYPE, lincolnMemorialJson, aVis);
        lincolnMemorialDocWithVis = EzElasticTestUtils
                .generateDocument(TEST_TYPE, lincolnMemorialJson, new Visibility().setFormalVisibility("C|E"));

        // White House
        whiteHouse = new PlaceOfInterest();
        whiteHouse.setTitle("White House");
        whiteHouse.setComments(
                "The home of the president of the US located at 1600 Pennsylvania Ave. Since 1800 it has "
                        + "been the home of every US president.");
        whiteHouse.setTags(Sets.newHashSet("dc", "white", "monument"));
        whiteHouse.setLocation(new Location(38.8977, -77.0365));
        whiteHouse.setRating(23);
        visit.add(Calendar.DAY_OF_MONTH, -3);
        whiteHouse.setVisit(visit.getTime());
        String whiteHouseJson = gson.toJson(whiteHouse);
        whiteHouseDoc = EzElasticTestUtils.generateDocument(TEST_TYPE, whiteHouseJson, aVis);
        whiteHouseDocWithVis = EzElasticTestUtils
                .generateDocument(TEST_TYPE, whiteHouseJson, new Visibility().setFormalVisibility("A&(B|E)"));

        populateWithTestDocs();
    }

    @Test
    public void testPutOne() throws Exception {
        esSetup.execute(EsSetup.deleteAll());
        setMappingForTest();

        final List<IndexResponse> result = client.put(Collections.singletonList(jeffersonMemorialDoc));

        // We're only indexing a single documents
        assertEquals(1, result.size());

        // This should be the first time the document was indexed
        assertEquals(1, result.get(0).get_version());

        // Should have been written to the correct type
        assertEquals(TEST_TYPE, result.get(0).get_type());
    }

    @Test
    public void testPutInvalid() {
        final Document testDoc = EzElasticTestUtils
                .generateDocument(TEST_TYPE, "{ THIS OBJECT ISN'T WELL FORMED: JSON : IS IT? :}", new Visibility());

        final List<IndexResponse> result = client.put(Collections.singletonList(testDoc));

        assertEquals(1, result.size());
        for (final IndexResponse response : result) {
            // The index should have failed
            assertFalse(response.isSuccess());
        }
    }

    @Test
    public void testPutMany() throws Exception {
        esSetup.execute(EsSetup.deleteAll());
        setMappingForTest();

        final List<IndexResponse> result = client.put(Arrays.asList(lincolnMemorialDoc, columbiaDoc, whiteHouseDoc));

        assertEquals(3, result.size());
        for (final IndexResponse response : result) {
            // This should be the first time the document was indexed
            assertEquals(1, response.get_version());

            // Should have been written to the correct type
            assertEquals(TEST_TYPE, response.get_type());
        }
    }

    @Test
    public void testUpdateScript() throws Exception {
        checkUpdateScript(columbia, columbiaDoc, COMMON_USER_TOKEN);

        try {
            // Given auths can't see requested doc
            checkUpdateScript(columbia, columbiaDocWithVis, COMMON_USER_TOKEN);
            fail("Expected exception not thrown");
        } catch (TException e) {
            // Expected
        }

        // With matching auths
        checkUpdateScript(columbia, columbiaDocWithVis, TestUtils.createTestToken("A", "B", "E"));
    }

    @Test
    public void testGetSingleById() throws Exception {
        final List<Document> resultsCommonVisMatching =
                client.get(ImmutableSet.of(whiteHouseDoc.get_id()), TEST_TYPE, COMMON_USER_TOKEN);

        assertEquals(1, resultsCommonVisMatching.size());
        assertEquals(
                whiteHouse, gson.fromJson(resultsCommonVisMatching.get(0).get_jsonObject(), PlaceOfInterest.class));

        final List<Document> resultsVisMismatch =
                client.get(ImmutableSet.of(whiteHouseDocWithVis.get_id()), TEST_TYPE, COMMON_USER_TOKEN);

        assertTrue(resultsVisMismatch.isEmpty());

        final List<Document> resultsVisMatching = client.get(
                ImmutableSet.of(whiteHouseDocWithVis.get_id()), TEST_TYPE, TestUtils.createTestToken("A", "E"));

        assertEquals(1, resultsVisMatching.size());
        assertEquals(
                whiteHouse, gson.fromJson(resultsVisMatching.get(0).get_jsonObject(), PlaceOfInterest.class));
    }

    @Test
    public void testGetFields() throws Exception {
        final Document result = client.get(
                whiteHouseDoc.get_id(), TEST_TYPE, Sets.newHashSet("title", "rating", "location"), COMMON_USER_TOKEN);

        final Map<String, Object> resultMap = EzElasticTestUtils.jsonToMap(result.get_jsonObject());

        assertEquals(3, resultMap.size());
        assertTrue(resultMap.containsKey("title"));
        assertEquals(whiteHouse.getTitle(), resultMap.get("title"));
        assertTrue(resultMap.containsKey("rating"));
        assertEquals(whiteHouse.getRating(), Integer.parseInt(resultMap.get("rating").toString()));
        assertTrue(resultMap.containsKey("location"));
        assertEquals(whiteHouse.getLocation(), gson.fromJson(resultMap.get("location").toString(), Location.class));
    }

    @Test(expected = MalformedQueryException.class)
    public void testQueryMalformed() throws Exception {
        final String malformedQuery = "I CAN HAZ LUCENE :-)";
        client.get(malformedQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);
    }

    @Test
    public void testQueryMatchOr() throws Exception {
        final String matchTitleQuery = QueryBuilders.matchQuery("title", "Memorial").toString();

        final SearchResult resultsCommonVis =
                client.get(matchTitleQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(
                Sets.newHashSet(jeffersonMemorialDoc.get_id(), lincolnMemorialDoc.get_id()),
                docListToIdsSet(resultsCommonVis.getMatchingDocuments()));

        final SearchResult resultsVis1 = client.get(
                matchTitleQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B", "C"));

        assertEquals(
                Sets.newHashSet(
                        jeffersonMemorialDoc.get_id(), lincolnMemorialDoc.get_id(),
                        jeffersonMemorialDocWithVis.get_id(), lincolnMemorialDocWithVis.get_id()),
                docListToIdsSet(resultsVis1.getMatchingDocuments()));

        final SearchResult resultsVis2 = client.get(
                matchTitleQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B", "E"));

        assertEquals(
                Sets.newHashSet(
                        jeffersonMemorialDoc.get_id(), lincolnMemorialDoc.get_id(), lincolnMemorialDocWithVis.get_id()),
                docListToIdsSet(resultsVis2.getMatchingDocuments()));
    }

    @Test
    public void testQueryMatchAnd() throws Exception {
        final String matchTitleQuery =
                QueryBuilders.matchQuery("title", "Columbia").operator(MatchQueryBuilder.Operator.AND).toString();

        final SearchResult resultsCommonVis =
                client.get(matchTitleQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(Sets.newHashSet(columbiaDoc.get_id()), docListToIdsSet(resultsCommonVis.getMatchingDocuments()));

        final SearchResult resultsVis1 = client.get(
                matchTitleQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B", "E"));

        assertEquals(
                Sets.newHashSet(columbiaDoc.get_id(), columbiaDocWithVis.get_id()),
                docListToIdsSet(resultsVis1.getMatchingDocuments()));
    }

    @Test
    public void testQueryMatchAndLucene() throws Exception {
        final String luceneQuery = "title:Jefferson AND title:Memorial";

        final SearchResult resultsCommonVis =
                client.get(luceneQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(
                Sets.newHashSet(jeffersonMemorialDoc.get_id()),
                docListToIdsSet(resultsCommonVis.getMatchingDocuments()));

        final SearchResult resultsVis1 = client.get(
                luceneQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B", "D"));

        assertEquals(
                Sets.newHashSet(jeffersonMemorialDoc.get_id(), jeffersonMemorialDocWithVis.get_id()),
                docListToIdsSet(resultsVis1.getMatchingDocuments()));
    }

    @Test
    public void testQueryMatchInt() throws Exception {
        final String matchRatingQuery = QueryBuilders.matchQuery("rating", 23).toString();

        final SearchResult resultsCommonVis =
                client.get(matchRatingQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(Sets.newHashSet(whiteHouseDoc.get_id()), docListToIdsSet(resultsCommonVis.getMatchingDocuments()));

        final SearchResult resultsVis1 = client.get(
                matchRatingQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "E"));

        assertEquals(
                Sets.newHashSet(whiteHouseDoc.get_id(), whiteHouseDocWithVis.get_id()),
                docListToIdsSet(resultsVis1.getMatchingDocuments()));
    }

    @Test
    public void testQueryMatchIntLucene() throws Exception {
        final String matchRatingQuery = "rating:23";

        final SearchResult resultsCommonVis =
                client.get(matchRatingQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(Sets.newHashSet(whiteHouseDoc.get_id()), docListToIdsSet(resultsCommonVis.getMatchingDocuments()));

        final SearchResult resultsVis1 = client.get(
                matchRatingQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "E"));

        assertEquals(
                Sets.newHashSet(whiteHouseDoc.get_id(), whiteHouseDocWithVis.get_id()),
                docListToIdsSet(resultsVis1.getMatchingDocuments()));
    }

    @Test
    public void testQueryMatchNone() throws Exception {
        final String noMatchesTitleQuery = QueryBuilders.matchQuery("title", "cow").toString();

        final SearchResult results = client.get(
                noMatchesTitleQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(0, results.getTotalHits());
    }

    @Test
    public void testQueryMatchNonExistentField() throws Exception {
        final String nonExistentFieldQuery = QueryBuilders.matchQuery("age", 23).toString();

        final SearchResult results = client.get(
                nonExistentFieldQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(0, results.getTotalHits());
    }

    @Test
    public void testQueryMatchNonExistentFieldLucene() throws Exception {
        final String nonExistentFieldQuery = "age:23";

        final SearchResult results = client.get(
                nonExistentFieldQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(0, results.getTotalHits());
    }

    @Test
    public void testQueryMultiMatch() throws Exception {
        final String multiMatchQuery = QueryBuilders.multiMatchQuery("white", "title", "comments").toString();

        final SearchResult resultsCommonVis =
                client.get(multiMatchQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(
                Sets.newHashSet(whiteHouseDoc.get_id(), lincolnMemorialDoc.get_id()),
                docListToIdsSet(resultsCommonVis.getMatchingDocuments()));

        final SearchResult resultsCommonVis1 = client.get(
                multiMatchQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B"));

        assertEquals(
                Sets.newHashSet(whiteHouseDoc.get_id(), lincolnMemorialDoc.get_id(), whiteHouseDocWithVis.get_id()),
                docListToIdsSet(resultsCommonVis1.getMatchingDocuments()));

        final SearchResult resultsCommonVis2 = client.get(
                multiMatchQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "E"));

        assertEquals(
                Sets.newHashSet(
                        whiteHouseDoc.get_id(), lincolnMemorialDoc.get_id(), whiteHouseDocWithVis.get_id(),
                        lincolnMemorialDocWithVis.get_id()), docListToIdsSet(resultsCommonVis2.getMatchingDocuments()));
    }

    @Test
    public void testQueryMultiMatchLucene() throws Exception {
        final String multiMatchQuery = "title:white OR comments:white";

        final SearchResult resultsCommonVis =
                client.get(multiMatchQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(
                Sets.newHashSet(whiteHouseDoc.get_id(), lincolnMemorialDoc.get_id()),
                docListToIdsSet(resultsCommonVis.getMatchingDocuments()));

        final SearchResult resultsVis1 = client.get(
                multiMatchQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B"));

        assertEquals(
                Sets.newHashSet(whiteHouseDoc.get_id(), lincolnMemorialDoc.get_id(), whiteHouseDocWithVis.get_id()),
                docListToIdsSet(resultsVis1.getMatchingDocuments()));

        final SearchResult resultsVis2 = client.get(
                multiMatchQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "E"));

        assertEquals(
                Sets.newHashSet(
                        whiteHouseDoc.get_id(), lincolnMemorialDoc.get_id(), whiteHouseDocWithVis.get_id(),
                        lincolnMemorialDocWithVis.get_id()), docListToIdsSet(resultsVis2.getMatchingDocuments()));
    }

    @Test
    public void testQueryRangeInt() throws Exception {
        final String rangeQuery = QueryBuilders.rangeQuery("rating").gt(20).lte(100).toString();

        final SearchResult resultsCommonVis =
                client.get(rangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(
                Sets.newHashSet(columbiaDoc.get_id(), whiteHouseDoc.get_id(), jeffersonMemorialDoc.get_id()),
                docListToIdsSet(resultsCommonVis.getMatchingDocuments()));

        final SearchResult resultsVis1 = client.get(
                rangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B", "C"));

        assertEquals(
                Sets.newHashSet(
                        columbiaDoc.get_id(), whiteHouseDoc.get_id(), jeffersonMemorialDoc.get_id(),
                        whiteHouseDocWithVis.get_id(), jeffersonMemorialDocWithVis.get_id()),
                docListToIdsSet(resultsVis1.getMatchingDocuments()));

        final SearchResult resultsVis2 = client.get(
                rangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B", "E"));

        assertEquals(
                Sets.newHashSet(
                        columbiaDoc.get_id(), whiteHouseDoc.get_id(), jeffersonMemorialDoc.get_id(),
                        columbiaDocWithVis.get_id(), whiteHouseDocWithVis.get_id()),
                docListToIdsSet(resultsVis2.getMatchingDocuments()));
    }

    @Test
    public void testQueryRangeIntLucene() throws Exception {
        final String rangeQuery = "rating:[20 TO 100]";

        final SearchResult resultsCommonVis =
                client.get(rangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(
                Sets.newHashSet(columbiaDoc.get_id(), whiteHouseDoc.get_id(), jeffersonMemorialDoc.get_id()),
                docListToIdsSet(resultsCommonVis.getMatchingDocuments()));

        final SearchResult resultsVis1 = client.get(
                rangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B", "C"));

        assertEquals(
                Sets.newHashSet(
                        columbiaDoc.get_id(), whiteHouseDoc.get_id(), jeffersonMemorialDoc.get_id(),
                        whiteHouseDocWithVis.get_id(), jeffersonMemorialDocWithVis.get_id()),
                docListToIdsSet(resultsVis1.getMatchingDocuments()));

        final SearchResult resultsVis2 = client.get(
                rangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B", "E"));

        assertEquals(
                Sets.newHashSet(
                        columbiaDoc.get_id(), whiteHouseDoc.get_id(), jeffersonMemorialDoc.get_id(),
                        columbiaDocWithVis.get_id(), whiteHouseDocWithVis.get_id()),
                docListToIdsSet(resultsVis2.getMatchingDocuments()));
    }

    @Test
    public void testQueryRangeDateNoMapping() throws Exception {
        client.setTypeMapping(TEST_TYPE, "");

        final Calendar now = new GregorianCalendar();
        now.add(Calendar.HOUR, -1);
        final Long oneHourAgo = now.getTimeInMillis();
        now.add(Calendar.HOUR, 25);
        final Long oneDayFromNow = now.getTimeInMillis();

        final String dateRangeQuery = QueryBuilders.rangeQuery("visit").gt(oneHourAgo).lt(oneDayFromNow).toString();
        final SearchResult results =
                client.get(dateRangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(0, results.getTotalHits());
    }

    @Test
    public void testQueryRangeDateMapped() throws Exception {
        final SimpleDateFormat dtg = new SimpleDateFormat(EzElasticTestUtils.DATE_FORMAT);
        final Calendar now = Calendar.getInstance();
        now.add(Calendar.HOUR, 1);
        final Date oneHourFromNow = now.getTime();
        now.add(Calendar.DAY_OF_MONTH, -1);
        final Date oneDayAgo = now.getTime();

        final String dateRangeQuery =
                QueryBuilders.rangeQuery("visit").gt(dtg.format(oneDayAgo)).lt(dtg.format(oneHourFromNow)).toString();

        final SearchResult resultsCommonVis =
                client.get(dateRangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(
                Sets.newHashSet(columbiaDoc.get_id(), lincolnMemorialDoc.get_id(), jeffersonMemorialDoc.get_id()),
                docListToIdsSet(resultsCommonVis.getMatchingDocuments()));

        final SearchResult resultsVis1 = client.get(
                dateRangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B", "C"));

        assertEquals(
                Sets.newHashSet(
                        columbiaDoc.get_id(), lincolnMemorialDoc.get_id(), jeffersonMemorialDoc.get_id(),
                        lincolnMemorialDocWithVis.get_id(), jeffersonMemorialDocWithVis.get_id()),
                docListToIdsSet(resultsVis1.getMatchingDocuments()));

        final SearchResult resultsVis2 = client.get(
                dateRangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B", "E"));

        assertEquals(
                Sets.newHashSet(
                        columbiaDoc.get_id(), lincolnMemorialDoc.get_id(), jeffersonMemorialDoc.get_id(),
                        columbiaDocWithVis.get_id(), lincolnMemorialDocWithVis.get_id()),
                docListToIdsSet(resultsVis2.getMatchingDocuments()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testQueryOnFields() throws Exception {
        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, Sets.newHashSet("title", "rating"), null,
                null, 0, (short) 10, null, COMMON_USER_TOKEN);

        assertEquals(4, results.getTotalHits());
        assertEquals(4, results.getMatchingDocumentsSize());
        Map<String, Object> map = new HashMap<>();
        for (final Document result : results.getMatchingDocuments()) {
            map = gson.fromJson(result.get_jsonObject(), map.getClass());
            assertTrue(map.keySet().contains("title"));
            assertTrue(map.keySet().contains("rating"));
        }
    }

    @Test
    public void testDateHistogramFacet() throws Exception {
        final List<Facet> facets = new ArrayList<>();
        final DateField dateField = new DateField();
        dateField.set_field("visit");
        final DateInterval dateInterval = new DateInterval();
        dateInterval.setStaticInterval(DateIntervalType.DAY);
        final DateHistogramFacet dhgFacet = new DateHistogramFacet(dateField, dateInterval);
        final FacetRequest request = new FacetRequest();
        request.setDateHistogramFacet(dhgFacet);
        facets.add(new Facet("Visit", request));

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, facets, null, 0, (short) 10, null,
                COMMON_USER_TOKEN);

        assertEquals(4, results.getTotalHits());
        assertFalse(results.getFacets().isEmpty());
        assertTrue(results.getFacets().containsKey("Visit"));
        int dateFacetsEntriesSize = results.getFacets().get("Visit").getDateFacetResult().getEntriesSize();
        assertTrue(dateFacetsEntriesSize > 1 && dateFacetsEntriesSize < 4);
    }

    @Test
    public void testRangeDateFacets() throws Exception {
        final SimpleDateFormat dtg = new SimpleDateFormat(EzElasticTestUtils.DATE_FORMAT);
        Calendar calendar = new GregorianCalendar();

        calendar.add(Calendar.DAY_OF_YEAR, -1);
        long last24Time = DateUtils.round(calendar, Calendar.HOUR).getTimeInMillis();

        calendar.add(Calendar.DAY_OF_YEAR, -1);
        long last48Time = DateUtils.round(calendar, Calendar.HOUR).getTimeInMillis();

        calendar.add(Calendar.DAY_OF_YEAR, -1);
        long last72Time = DateUtils.round(calendar, Calendar.HOUR).getTimeInMillis();

        List<Facet> rangeDateFacets = Collections.singletonList(
                EzElasticTestUtils.generateDateBucketFacet(
                        last24Time, last48Time, last72Time));

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, rangeDateFacets, null, 0, (short) 10,
                null, COMMON_USER_TOKEN);

        assertEquals(4, results.getTotalHits());
        assertFalse(results.getFacets().isEmpty());
        assertTrue(results.getFacets().containsKey("Report Date"));
        final FacetResult facetResult = results.getFacets().get("Report Date");
        for (final RangeFacetEntry entry : facetResult.getRangeFacetResult().getEntries()) {
            if (dtg.parse(entry.getRangeFrom()).getTime() >= last24Time
                    || dtg.parse(entry.getRangeFrom()).getTime() >= last48Time
                    || dtg.parse(entry.getRangeFrom()).getTime() >= last72Time) {
                assertEquals(3, entry.getCount());
            } else {
                assertEquals(4, entry.getCount());
            }
        }
    }

    @Test
    public void testDelete() throws Exception {
        client.delete(ImmutableSet.of(whiteHouseDoc.get_id()), TEST_TYPE, COMMON_USER_TOKEN);

        final List<Document> resultsCommonVis =
                client.get(ImmutableSet.of(whiteHouseDoc.get_id()), TEST_TYPE, COMMON_USER_TOKEN);

        assertTrue(resultsCommonVis.isEmpty());

        client.delete(ImmutableSet.of(whiteHouseDocWithVis.get_id()), TEST_TYPE, COMMON_USER_TOKEN);

        final List<Document> resultsVisMismatch = client.get(
                ImmutableSet.of(whiteHouseDocWithVis.get_id()), TEST_TYPE, TestUtils.createTestToken("A", "E"));

        assertEquals(Sets.newHashSet(whiteHouseDocWithVis.get_id()), docListToIdsSet(resultsVisMismatch));

        client.delete(ImmutableSet.of(whiteHouseDocWithVis.get_id()), TEST_TYPE, TestUtils.createTestToken("A", "E"));

        final List<Document> resultsVisMatching = client.get(
                ImmutableSet.of(whiteHouseDocWithVis.get_id()), TEST_TYPE, TestUtils.createTestToken("A", "E"));

        assertTrue(resultsVisMatching.isEmpty());
    }

    @Test
    public void testHighlighting() throws Exception {
        final HighlightRequest highlight = new HighlightRequest().setFields(
                Sets.newHashSet(new HighlightedField("comments"))).setPre_tags(Collections.singletonList("FOO>>"))
                .setPost_tags(Collections.singletonList("<<BAR"));

        final SearchResult result = client.get(
                QueryBuilders.matchQuery("comments", "white").toString(), TEST_TYPE, null, null, null, null, 0,
                (short) -1, highlight, COMMON_USER_TOKEN);

        assertEquals(1, result.getTotalHits());

        final Map<String, List<String>> highlights =
                result.getHighlights().get(lincolnMemorialDoc.get_id()).getResults();

        assertNotNull(highlights);

        assertEquals(
                "The Lincoln Memorial is an American national monument built to honor the 16th President of the "
                        + "United States, Abraham Lincoln. Made of FOO>>white<<BAR stone.",
                highlights.get("comments").get(0));
    }

    @Test
    public void testFacetSize() throws Exception {
        final TermsFacet termsFacetWithDefaultSize = new TermsFacet();
        termsFacetWithDefaultSize.setFields(Collections.singletonList("rating"));
        final FacetRequest requestWithDefaultSize = new FacetRequest();
        requestWithDefaultSize.setTermsFacet(termsFacetWithDefaultSize);

        final SearchResult resultsWithDefaultSize = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null,
                Collections.singletonList(new Facet("Rating", requestWithDefaultSize)), null, 0, (short) 10, null,
                COMMON_USER_TOKEN);

        assertEquals(4, resultsWithDefaultSize.getTotalHits());
        assertFalse(resultsWithDefaultSize.getFacets().isEmpty());
        assertTrue(resultsWithDefaultSize.getFacets().containsKey("Rating"));
        assertEquals(3, resultsWithDefaultSize.getFacets().get("Rating").getTermsFacetResult().getEntriesSize());

        final TermsFacet termsFacetWithSetSize = new TermsFacet();
        termsFacetWithSetSize.setFields(Collections.singletonList("rating"));
        termsFacetWithSetSize.setSize(2);
        final FacetRequest requestWithSetSize = new FacetRequest();
        requestWithSetSize.setTermsFacet(termsFacetWithSetSize);

        final SearchResult resultsWithSetSize = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null,
                Collections.singletonList(new Facet("Rating", requestWithSetSize)), null, 0, (short) 10, null,
                COMMON_USER_TOKEN);

        assertEquals(4, resultsWithSetSize.getTotalHits());
        assertFalse(resultsWithSetSize.getFacets().isEmpty());
        assertTrue(resultsWithSetSize.getFacets().containsKey("Rating"));
        assertEquals(2, resultsWithSetSize.getFacets().get("Rating").getTermsFacetResult().getEntriesSize());
    }

    @Test
    public void testSortInteger() throws Exception {
        final FieldSort ratingDescending = new FieldSort();
        ratingDescending.setField("rating");
        ratingDescending.setOrder(SortOrder.DESCENDING);
        final SortCriteria sortDescendingCriteria = new SortCriteria();
        sortDescendingCriteria.setFieldSort(ratingDescending);

        final SearchResult descendingResults = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, Collections.singletonList(sortDescendingCriteria),
                null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        final List<String> expectedIds = Arrays.asList(
                columbiaDoc.get_id(), whiteHouseDoc.get_id(), jeffersonMemorialDoc.get_id(),
                lincolnMemorialDoc.get_id());

        assertEquals(expectedIds.size(), descendingResults.getTotalHits());
        final List<Integer> descendingRatings = new ArrayList<>((int) descendingResults.getTotalHits());
        for (final Document result : descendingResults.getMatchingDocuments()) {
            final PlaceOfInterest fromJson = gson.fromJson(result.get_jsonObject(), PlaceOfInterest.class);
            descendingRatings.add(fromJson.getRating());
        }

        final Ordering<Integer> descendingOrdering = Ordering.natural().reverse();
        assertTrue(descendingOrdering.isOrdered(descendingRatings));

        final FieldSort sortAscending = new FieldSort();
        sortAscending.setField("rating");
        sortAscending.setOrder(SortOrder.ASCENDING);
        final SortCriteria sortAscendingCriteria = new SortCriteria();
        sortAscendingCriteria.setFieldSort(sortAscending);

        final SearchResult ascendingResults = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, Collections.singletonList(sortAscendingCriteria),
                null, null, null, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(expectedIds.size(), ascendingResults.getTotalHits());
        final List<Integer> ascendingRatings = new ArrayList<>((int) ascendingResults.getTotalHits());
        for (final Document result : ascendingResults.getMatchingDocuments()) {
            final PlaceOfInterest fromJson = gson.fromJson(result.get_jsonObject(), PlaceOfInterest.class);
            ascendingRatings.add(fromJson.getRating());
        }

        final Ordering<Integer> ascendingOrdering = Ordering.natural();
        assertTrue(ascendingOrdering.isOrdered(ascendingRatings));
    }

    @Test
    public void testQueryNonExistentIndex() throws Exception {
        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), "notExists", null, null, null, null, 0, (short) 10, null,
                COMMON_USER_TOKEN);

        assertEquals(0, results.getMatchingDocuments().size());
        assertEquals(QueryBuilders.matchAllQuery().toString(), results.actualQuery);
    }

    @Test
    public void testQueryWithFilter() throws Exception {
        final String filter = FilterBuilders.andFilter(
                FilterBuilders.rangeFilter("rating").from(0).to(100), FilterBuilders.termFilter("title", "memorial"))
                .toString();

        final SearchResult resultsCommonVis = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, null, filter, 0, (short) -1, null,
                COMMON_USER_TOKEN);

        assertEquals(
                Sets.newHashSet(jeffersonMemorialDoc.get_id(), lincolnMemorialDoc.get_id()),
                docListToIdsSet(resultsCommonVis.getMatchingDocuments()));

        final SearchResult resultsVis1 = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, null, filter, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B", "C"));

        assertEquals(
                Sets.newHashSet(
                        jeffersonMemorialDoc.get_id(), lincolnMemorialDoc.get_id(), lincolnMemorialDocWithVis.get_id(),
                        jeffersonMemorialDocWithVis.get_id()), docListToIdsSet(resultsVis1.getMatchingDocuments()));

        final SearchResult resultsVis2 = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, null, filter, 0, (short) -1, null,
                TestUtils.createTestToken("C"));

        assertEquals(
                Sets.newHashSet(lincolnMemorialDocWithVis.get_id()),
                docListToIdsSet(resultsVis2.getMatchingDocuments()));
    }

    @Test
    public void testQueryWithElasticJsonAndFilter() throws Exception {
        final String filter = FilterBuilders.andFilter(
                FilterBuilders.rangeFilter("rating").from(0).to(100), FilterBuilders.termFilter("title", "memorial"))
                .toString();

        final String query = QueryBuilders.boolQuery().must(
                QueryBuilders.rangeQuery("rating").from(35).to(98).includeLower(true).includeLower(true))
                .must(QueryBuilders.matchQuery("comments", "memorial")).mustNot(QueryBuilders.prefixQuery("title", "W"))
                .toString();

        final SearchResult resultsCommonVis =
                client.get(query, TEST_TYPE, null, null, null, filter, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(
                Sets.newHashSet(jeffersonMemorialDoc.get_id()),
                docListToIdsSet(resultsCommonVis.getMatchingDocuments()));

        final SearchResult resultsVis1 = client.get(
                query, TEST_TYPE, null, null, null, filter, 0, (short) -1, null,
                TestUtils.createTestToken("A", "B", "C"));

        assertEquals(
                Sets.newHashSet(jeffersonMemorialDoc.get_id(), jeffersonMemorialDocWithVis.get_id()),
                docListToIdsSet(resultsVis1.getMatchingDocuments()));
    }

    @Test
    public void testTermScript() throws Exception {
        final TermsScriptFacet tsf = new TermsScriptFacet();
        tsf.setFields(Collections.singletonList("title"));
        tsf.setScript(new ValueScript("term == 'memorial' ? 'memorial' : 'other'", new ArrayList<ScriptParam>()));
        final FacetRequest request = new FacetRequest();
        request.setTermsScriptFacet(tsf);
        final Facet facet = new Facet();
        facet.setLabel("magic");
        facet.setFacet(request);

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, Collections.singletonList(facet), null,
                0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(4, results.getTotalHits());
        int otherCount = 0;
        int memorialCount = 0;

        for (final Document result : results.getMatchingDocuments()) {
            final PlaceOfInterest fromJson = gson.fromJson(result.get_jsonObject(), PlaceOfInterest.class);
            for (final String val : fromJson.getTitle().split(" ")) {
                if ("memorial".equalsIgnoreCase(val)) {
                    memorialCount++;
                } else {
                    otherCount++;
                }
            }
        }

        for (final TermsFacetEntry facetResult : results.getFacets().get("magic").getTermsFacetResult().getEntries()) {
            if ("memorial".equalsIgnoreCase(facetResult.getTerm())) {
                assertEquals(memorialCount, facetResult.getCount());
            } else {
                assertEquals(otherCount, facetResult.getCount());
            }
        }
    }

    @Test
    public void testTermsStatsStringFacet() throws Exception {
        final TermsStatsValue val = new TermsStatsValue();
        val.setValueField("rating");
        final TermsStatsFacet tsf = new TermsStatsFacet();
        tsf.setKeyField("title");
        tsf.setValueField(val);
        final FacetRequest request = new FacetRequest();
        request.setTermsStatsFacet(tsf);
        final Facet facet = new Facet();
        facet.setLabel("magic");
        facet.setFacet(request);

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, Collections.singletonList(facet), null,
                0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(4, results.getTotalHits());
        for (final TermsStatsFacetResultEntry facetResult : results.getFacets().get("magic").getTermsStatsFacetResult()
                .getEntries()) {
            assertNotEquals(facetResult.getTerm(), Integer.toString((int) facetResult.getTermAsNumber()));
        }
    }

    @Test
    public void testTermsStatsDoubleFacet() throws Exception {
        final TermsStatsValue val = new TermsStatsValue();
        val.setValueField("rating");
        final TermsStatsFacet tsf = new TermsStatsFacet();
        tsf.setKeyField("rating");
        tsf.setValueField(val);
        final FacetRequest request = new FacetRequest();
        request.setTermsStatsFacet(tsf);
        final Facet facet = new Facet();
        facet.setLabel("magic");
        facet.setFacet(request);

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, Collections.singletonList(facet), null,
                0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(4, results.getTotalHits());
        for (final TermsStatsFacetResultEntry facetResult : results.getFacets().get("magic").getTermsStatsFacetResult()
                .getEntries()) {
            assertEquals(facetResult.getTerm(), Integer.toString((int) facetResult.getTermAsNumber()));
        }
    }

    @Test
    public void testTermsStatsFacetWithFilter() throws Exception {
        final String filter = FilterBuilders.andFilter(
                FilterBuilders.rangeFilter("rating").from(0).to(100), FilterBuilders.termFilter("title", "memorial"))
                .toString();

        final TermsStatsValue val = new TermsStatsValue();
        val.setValueField("rating");
        final TermsStatsFacet tsf = new TermsStatsFacet();
        tsf.setKeyField("title");
        tsf.setValueField(val);
        final FacetRequest request = new FacetRequest();
        request.setTermsStatsFacet(tsf);
        final Facet facet = new Facet();
        facet.setLabel("magic");
        facet.setFacet(request);
        facet.setFilterJSON(filter);

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, Collections.singletonList(facet), null,
                0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(4, results.getTotalHits());
        final Set<String> uniqueTitleTerms = new HashSet<>();
        uniqueTitleTerms.addAll(Arrays.asList(lincolnMemorial.getTitle().split(" ")));
        uniqueTitleTerms.addAll(Arrays.asList(jeffersonMemorial.getTitle().split(" ")));

        assertEquals(
                uniqueTitleTerms.size(),
                results.getFacets().get("magic").getTermsStatsFacetResult().getEntries().size());
    }

    @Test
    public void testFilterShouldNotFilterFacets() throws Exception {
        final String filter = FilterBuilders.termFilter("title", "memorial").toString();

        final TermsFacet tf = new TermsFacet(Collections.singletonList("rating"));
        final FacetRequest request = new FacetRequest();
        request.setTermsFacet(tf);
        final Facet facet = new Facet().setLabel("magic").setFacet(request);

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, Collections.singletonList(facet),
                filter, 0, (short) -1, null, COMMON_USER_TOKEN);

        assertEquals(2, results.getTotalHits());
        final List<TermsFacetEntry> entries = results.getFacets().get("magic").getTermsFacetResult().getEntries();
        for (final TermsFacetEntry entry : entries) {
            // if the filter gets applied to the facets this will only be 1
            if ("92".equalsIgnoreCase(entry.getTerm())) {
                assertEquals(2, entry.getCount());
            }
        }
    }

    @Test
    public void testPurge() throws Exception {
        final Visibility visibility = new Visibility().setFormalVisibility("A")
                .setAdvancedMarkings(new AdvancedMarkings().setId(5L).setComposite(false));

        final Document purgeDoc =
                EzElasticTestUtils.generateDocument(TEST_TYPE, gson.toJson(jeffersonMemorial), visibility);

        client.put(Arrays.asList(purgeDoc, lincolnMemorialDoc));
        client.forceIndexRefresh();

        // Ensure it exists before purging
        final List<Document> results = client.get(ImmutableSet.of(purgeDoc.get_id()), TEST_TYPE, COMMON_USER_TOKEN);
        assertEquals(purgeDoc.get_id(), results.get(0).get_id());

        final PurgeResult purgeResult = client.purge(79472397424L, Sets.newHashSet(5L, 25L), 100);
        assertNotNull(purgeResult.getPurged());
        assertTrue(purgeResult.getPurged().contains(5L));

        SearchResult afterPurgeResults = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                COMMON_USER_TOKEN);

        assertEquals(4, afterPurgeResults.getTotalHits());
        assertTrue(client.get(Sets.newHashSet(purgeDoc.get_id()), TEST_TYPE, COMMON_USER_TOKEN).isEmpty());
    }

    @Test
    public void testPercolate() throws Exception {
        PercolateQuery percolator = createTestPercolator("foo", new Visibility().setFormalVisibility("A"), null);
        final IndexResponse percolateResponse = client.putPercolator(percolator, COMMON_USER_TOKEN);
        assertTrue(percolateResponse.isSuccess());
        assertEquals("foo", percolateResponse.get_id());

        lincolnMemorialDoc.setPercolate(new PercolateRequest());
        columbiaDoc.setPercolate(new PercolateRequest());
        whiteHouseDoc.setPercolate(new PercolateRequest());
        jeffersonMemorialDoc.setPercolate(new PercolateRequest());

        final List<PercolateQuery> matches = client.percolate(
                Arrays.asList(lincolnMemorialDoc, columbiaDoc, whiteHouseDoc, jeffersonMemorialDoc), false,
                COMMON_USER_TOKEN);

        assertEquals(3, matches.size());

        assertEquals(
                1,
                client.get(Sets.newHashSet(percolateResponse.get_id()), ElasticUtils.PERCOLATOR_TYPE, COMMON_USER_TOKEN)
                        .size());

        client.deletePercolator(percolateResponse.get_id(), COMMON_USER_TOKEN);

        assertTrue(
                client.get(
                        Sets.newHashSet(percolateResponse.get_id()), ElasticUtils.PERCOLATOR_TYPE, COMMON_USER_TOKEN)
                        .isEmpty());

        final List<PercolateQuery> secondMatches = client.percolate(
                Arrays.asList(
                        lincolnMemorialDoc, columbiaDoc, whiteHouseDoc, jeffersonMemorialDoc), false,
                COMMON_USER_TOKEN);

        assertTrue(secondMatches.isEmpty());
    }

    @Test
    public void testPercolatePutSecurity() throws Exception {
        try {
            PercolateQuery percolator = createTestPercolator(
                    "foo", new Visibility().setFormalVisibility("A|B"),
                    new Authorizations().setFormalAuthorizations(Sets.newHashSet("B")));

            client.putPercolator(percolator, COMMON_USER_TOKEN);
            fail("Expected exception not thrown");
        } catch (TException e) {
            // Expected exception
        }

        try {
            PercolateQuery percolator = createTestPercolator(
                    "foo", new Visibility().setFormalVisibility("A|B"),
                    new Authorizations().setFormalAuthorizations(Sets.newHashSet("A", "B")));

            client.putPercolator(percolator, COMMON_USER_TOKEN);
            fail("Expected exception not thrown");
        } catch (TException e) {
            // Expected exception
        }

        PercolateQuery percWithDiffVis =
                createTestPercolator("percWithDiffVis", new Visibility().setFormalVisibility("B"), null);

        client.putPercolator(percWithDiffVis, COMMON_USER_TOKEN);

        assertTrue(
                client.get(Sets.newHashSet("percWithDiffVis"), ElasticUtils.PERCOLATOR_TYPE, COMMON_USER_TOKEN)
                        .isEmpty());

        assertEquals(
                1, client.get(
                        Sets.newHashSet("percWithDiffVis"), ElasticUtils.PERCOLATOR_TYPE,
                        TestUtils.createTestToken("B")).size());
    }

    @Test
    public void testPercolatorDeletionSecurity() throws Exception {
        EzSecurityToken matchingToken = TestUtils.createTestToken("B");
        String percolatorId = "toBeDeleted";

        PercolateQuery percolator = createTestPercolator(percolatorId, new Visibility().setFormalVisibility("B"), null);
        client.putPercolator(percolator, matchingToken);
        assertEquals(1, client.get(Sets.newHashSet(percolatorId), ElasticUtils.PERCOLATOR_TYPE, matchingToken).size());

        // Shouldn't be able to delete
        client.deletePercolator(percolatorId, COMMON_USER_TOKEN);
        assertEquals(1, client.get(Sets.newHashSet(percolatorId), ElasticUtils.PERCOLATOR_TYPE, matchingToken).size());

        // Should be able to delete
        client.deletePercolator(percolatorId, matchingToken);
        assertTrue(client.get(Sets.newHashSet(percolatorId), ElasticUtils.PERCOLATOR_TYPE, matchingToken).isEmpty());
    }

    @Test
    public void testPercolatorPercolationSecurity() throws Exception {
        EzSecurityToken matchingToken = TestUtils.createTestToken("A", "B", "E", "F");

        PercolateQuery percolator = createTestPercolator(
                "myPercolator", new Visibility().setFormalVisibility("A&B"),
                new Authorizations().setFormalAuthorizations(Sets.newHashSet("A", "B", "E")));

        IndexResponse response = client.putPercolator(percolator, matchingToken);
        assertTrue(response.isSuccess());
        assertEquals("myPercolator", response.get_id());

        lincolnMemorialDoc.setPercolate(new PercolateRequest());
        lincolnMemorialDocWithVis.setPercolate(new PercolateRequest());
        columbiaDoc.setPercolate(new PercolateRequest());
        columbiaDocWithVis.setPercolate(new PercolateRequest());
        whiteHouseDoc.setPercolate(new PercolateRequest());
        whiteHouseDocWithVis.setPercolate(new PercolateRequest());
        jeffersonMemorialDoc.setPercolate(new PercolateRequest());
        jeffersonMemorialDocWithVis.setPercolate(new PercolateRequest());

        List<Document> docsToPercolate = Lists.newArrayList(
                lincolnMemorialDoc, lincolnMemorialDocWithVis, columbiaDoc, columbiaDocWithVis, whiteHouseDoc,
                whiteHouseDocWithVis, jeffersonMemorialDoc, jeffersonMemorialDocWithVis);

        final List<PercolateQuery> matchesWithMatchingTokenNoPostFilter = client.percolate(
                docsToPercolate, false, matchingToken);

        assertEquals(5, matchesWithMatchingTokenNoPostFilter.size());

        final List<PercolateQuery> matchesWithMatchingTokenWithPostFilter = client.percolate(
                docsToPercolate, true, matchingToken);

        assertEquals(5, matchesWithMatchingTokenWithPostFilter.size());

        final List<PercolateQuery> matchesWithSubTokenNoPostFilter = client.percolate(
                docsToPercolate, false, COMMON_USER_TOKEN);

        assertEquals(5, matchesWithSubTokenNoPostFilter.size());

        final List<PercolateQuery> matchesWithSubTokenWithPostFilter = client.percolate(
                docsToPercolate, true, COMMON_USER_TOKEN);

        assertEquals(0, matchesWithSubTokenWithPostFilter.size());
    }

    private PercolateQuery createTestPercolator(String id, Visibility visibility, Authorizations auths)
            throws Exception {
        final FilterBuilder geoDistanceFilter =
                new GeoDistanceFilterBuilder(TEST_TYPE + ".location").distance(10, DistanceUnit.KILOMETERS)
                        .lat(whiteHouse.getLocation().getLat()).lon(whiteHouse.getLocation().getLon());

        final QueryBuilder filteredQuery = new FilteredQueryBuilder(QueryBuilders.matchAllQuery(), geoDistanceFilter);
        final String queryDoc = jsonBuilder().startObject().field("query", filteredQuery).endObject().string();

        return new PercolateQuery().setId(id).setQueryDocument(queryDoc).setVisibility(visibility)
                .setAuthorizations(auths);
    }

    private void populateWithTestDocs() {
        client.put(
                Arrays.asList(
                        lincolnMemorialDoc, columbiaDoc, whiteHouseDoc, jeffersonMemorialDoc, lincolnMemorialDocWithVis,
                        columbiaDocWithVis, whiteHouseDocWithVis, jeffersonMemorialDocWithVis));

        client.forceIndexRefresh();
    }

    private void setMappingForTest() throws IOException {
        final XContentBuilder mapping = jsonBuilder();
        mapping.startObject();
        mapping.startObject(TEST_TYPE);
        mapping.startObject("properties");

        mapping.startObject("title");
        mapping.field("type", "string");
        mapping.field("stored", "yes");
        mapping.endObject();

        mapping.startObject("comments");
        mapping.field("type", "string");
        mapping.endObject();

        mapping.startObject("tags");
        mapping.field("type", "string");
        mapping.field("index", "not_analyzed");
        mapping.endObject();

        mapping.startObject("location");
        mapping.field("type", "geo_point");
        mapping.endObject();

        mapping.startObject("rating");
        mapping.field("type", "integer");
        mapping.field("stored", "yes");
        mapping.endObject();

        mapping.startObject("visit");
        mapping.field("type", "date");
        mapping.field("format", EzElasticTestUtils.DATE_FORMAT);
        mapping.field("stored", "yes");
        mapping.endObject();

        mapping.endObject();
        mapping.endObject();
        mapping.endObject();

        client.setTypeMapping(TEST_TYPE, mapping.string());
    }

    private void checkUpdateScript(PlaceOfInterest poi, Document doc, EzSecurityToken token) throws Exception {
        final UpdateScript script = new UpdateScript().setScript("ctx._source.tags += newtag");
        script.putToParameters("newtag", "fun");

        final IndexResponse result = client.update(
                new DocumentIdentifier(doc.get_id()).setType(TEST_TYPE), script, new UpdateOptions(), token);

        assertEquals(doc.get_id(), result.get_id());
        final Document updated = client.get(doc.get_id(), doc.get_type(), null, token);

        final PlaceOfInterest updatedModel = gson.fromJson(updated.get_jsonObject(), PlaceOfInterest.class);
        assertTrue(updatedModel.getTags().containsAll(poi.getTags()));
        assertTrue(updatedModel.getTags().contains("fun"));
    }

    private Set<String> docListToIdsSet(List<Document> docs) {
        Iterable<String> idsIter = Iterables.transform(
                docs, new Function<Document, String>() {
                    @Override
                    public String apply(Document input) {
                        return input.get_id();
                    }
                });

        return Sets.newHashSet(idsIter);
    }
}
