package fr.xebia.xke.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import fr.xebia.xke.cassandra.model.Track;
import fr.xebia.xke.cassandra.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class CassandraRepository {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraRepository.class);

    private final Session session;

    public CassandraRepository(Session session) {
        this.session = session;
    }

    /**
     * Save an user by using a BoundStatement.
     * <p/>
     * http://www.datastax.com/doc-source/developer/java-driver/#quick_start/qsSimpleClientBoundStatements_t.html
     *
     * @param user the user to save
     */
    public void writeUserWithBoundStatement(User user) {
        PreparedStatement preparedStatement = session.prepare("INSERT INTO user (id, name, email, age) VALUES (?,?,?,?)");
        BoundStatement boundStatement = preparedStatement.bind(user.getId(), user.getName(), user.getEmail(), user.getAge());
        boundStatement.setConsistencyLevel(ConsistencyLevel.ANY);
        LOG.debug("insert user: {}", user);
        session.execute(boundStatement);
    }

    /**
     * Read a user with by using a QueryBuilder.
     * <p/>
     * http://www.datastax.com/doc-source/developer/java-driver/#reference/queryBuilder_r.html
     *
     * @param id the user id
     * @return a ResultSet of users
     */
    public ResultSet readUserWithQueryBuilder(UUID id) {
        return session.execute(select().all().from("user")
                .where(eq("id", id)));
    }

    /**
     * Insert a track with by using a QueryBuilder.
     * <p/>
     * http://www.datastax.com/doc-source/developer/java-driver/#reference/queryBuilder_r.html
     *
     * @param tracks track to save
     */
    public void writeTracksWithQueryBuilder(Iterable<Track> tracks) {
        for (Track track : tracks) {
            Query insert = insertInto("tracks")
                    .value("id", track.getId())
                    .value("title", track.getTitle())
                    .value("release", track.getRelease())
                    .value("duration", track.getDuration())
                    .value("tags", track.getTags());
            LOG.debug("insert track: {}", track);
            session.execute(insert);
        }
    }

    /**
     * Save an user click into a stream table.
     *
     * @param userId the user ID
     * @param when   the timestamp of the click
     * @param url    the url where the user has cliked
     * @param ttl    the Time To Live (TTL)
     * @see com.datastax.driver.core.querybuilder.Using
     */
    public void writeToClickStreamWithTTL(UUID userId, Date when, String url, Integer ttl) {
        LOG.debug("insert into user_click_stream [{}, {}]", userId, when.getTime());
        session.execute(insertInto("user_click_stream")
                .value("user_id", userId)
                .value("when", when)
                .value("url", url)
                .using(ttl(ttl)));
    }

    /**
     * Read user click stream between two dates.
     *
     * @param userId the user ID
     * @param start  start date of the time frame
     * @param end    end date of the time frame
     * @return a ResultSet of user click stream
     */
    public ResultSet readClickStreamByTimeframe(UUID userId, Date start, Date end) {
        return session.execute(select().from("user_click_stream")
                .where(eq("user_id", userId))
                .and(gt("when", start))
                .and(lt("when", end)));
    }

    /**
     * Execute an async query.
     * <p/>
     * http://www.datastax.com/doc-source/developer/java-driver/#asynchronous_t.html
     *
     * @param user   the user who like some tracks
     * @param tracks the list of tracks
     * @return a ResultSetFuture
     */
    public ResultSetFuture writeAndReadLikesAsynchronously(User user, Iterable<Track> tracks) {
        for (Track track : tracks) {
            session.executeAsync(
                    insertInto("track_likes")
                            .value("user_id", user.getId())
                            .value("track_id", track.getId())
            );
        }
        return session.executeAsync(
                select().all().from("track_likes").where(eq("user_id", user.getId())).limit(1000)
        );
    }


    /**
     * Batch query.
     *
     * @param insertQueries insert queries
     * @see QueryBuilder
     */
    public void batchWriteUsers(List<String> insertQueries) {
        Batch batch = batch();
        for (String insertQuery : insertQueries) {
            batch.add(new SimpleStatement(insertQuery));
        }
        batch.setConsistencyLevel(ConsistencyLevel.ALL).enableTracing();
        session.execute(batch);
    }
}
