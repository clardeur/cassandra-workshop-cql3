package fr.xebia.xke.cassandra.model;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Date;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Objects.toStringHelper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Track {

    @JsonProperty("artistMbid")
    private UUID id;

    @JsonProperty("name")
    private String title;

    private Date release;

    private Float duration;

    private Set<String> tags;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Date getRelease() {
        return release;
    }

    public void setRelease(Date release) {
        this.release = release;
    }

    public Float getDuration() {
        return duration;
    }

    public void setDuration(Float duration) {
        this.duration = duration;
    }

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Track track = (Track) o;

        if (duration != null ? !duration.equals(track.duration) : track.duration != null) return false;
        if (id != null ? !id.equals(track.id) : track.id != null) return false;
        if (title != null ? !title.equals(track.title) : track.title != null) return false;
        if (release != null ? !release.equals(track.release) : track.release != null) return false;
        if (tags != null ? !tags.equals(track.tags) : track.tags != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (release != null ? release.hashCode() : 0);
        result = 31 * result + (duration != null ? duration.hashCode() : 0);
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("id", id)
                .add("title", title)
                .add("release", release)
                .add("duration", duration)
                .add("tags", tags)
                .toString();
    }
}
