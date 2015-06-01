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

package ezbake.data.elastic.test;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Set;

import org.apache.commons.lang.time.DateUtils;

import com.google.common.collect.Sets;

public final class PlaceOfInterest {
    private String title;
    private String comments;
    private Set<String> tags;
    private Location location;
    private Date visit;
    private int rating;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) {
        this.tags = Collections.unmodifiableSet(Sets.newHashSet(tags));
    }

    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }

    public Date getVisit() {
        return (Date) visit.clone();
    }

    public void setVisit(Date visit) {
        this.visit = DateUtils.truncate(visit, Calendar.SECOND);
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    @Override
    public int hashCode() {
        int result = title != null ? title.hashCode() : 0;
        result = 31 * result + (comments != null ? comments.hashCode() : 0);
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        result = 31 * result + (location != null ? location.hashCode() : 0);
        result = 31 * result + (visit != null ? visit.hashCode() : 0);
        result = 31 * result + rating;

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final PlaceOfInterest that = (PlaceOfInterest) o;

        if (rating != that.rating) {
            return false;
        }

        if (comments != null ? !comments.equals(that.comments) : that.comments != null) {
            return false;
        }

        if (location != null ? !location.equals(that.location) : that.location != null) {
            return false;
        }

        if (tags != null ? !tags.equals(that.tags) : that.tags != null) {
            return false;
        }

        if (title != null ? !title.equals(that.title) : that.title != null) {
            return false;
        }

        if (visit != null ? !visit.equals(that.visit) : that.visit != null) {
            return false;
        }

        return true;
    }
}
