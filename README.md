## Overview
This project converts a Moodle archive (mbz), or a directory of XML files extracted from an mbz, 
into the equivalent dataset represented as sheets in a small set of Excel files.

## Requirements

Uses Python 3, Pandas and Jupyter notebooks
Optionally requires IP2Location csv data to convert IP into geo-location.


## Main Features

* In addition to native unix epoch timestamp, timestamps are recorded as readable UTC strings
* HTML content is extracted as plain text
* Text encoded as base-64 is decoded
* Moodle userid are converted into anonymous ids (that can be specified external or defined as random hext digits)
* user Iv4P addresses are optionally converted in geographic locations (town, state,country, lattitude and longitude etc)
* References to surrounding tags are automatically indexed
* Superfluous xml tags (that only act as a container) are not discarded
* Attributes and leaf tags are combined as columns for the table
* EOriginal xml source file is referenced in every Excel table row to aid cross referencing and validation.
* Data across multiple course section instances is aggregated together
* Similar object types across all components is aggregatedinto a single Excel table
* Input source can be the original mbz archive file, or the archive file already expanded as a list of xml files in multiple subdirectories.

## Use

1. If Geo-IP location is valuable to you. Download and unzip the free IPv4 city data from IP2Location.com
2. Specify the location of the mbz file (or the directory of expanded archive) - see the end of the notebook for configuration options
3. Run the entire notebook. Drink coffee or tea while it processes your data.

## Limitations and gotchas of current build

* Does not remap index and tag when superfluous containers are dropped. So it is not possible to determine next level container.
* Do not assume data is sorted by id or time.
* The assessment sheet (generated from workshop.xml) may generate URLs that are longer than 255 characters, the largested supported by Excel. These very long URLs will be excluded
* No verification or correctness testing of the data has been performed.
* It is unknown if the inferred timestamps based on the Unix Epoch timestamp require a timezone adjustment.


# Working with Moodle data
## Notes on extracting forum texts

ALL_Forum.xlsx  post table
message_text (which is the plain text version of the 'message' column, but with superfluous html tags removed)
anonid - the anonymized user (hex string)
parent (zero for a post with no parent, or equal to an 'id' in the same table)
id a unique integer
subject - seems to be original subject of firstpost

For top-level posts (i.e. parent = 0)
This maps to 
ALL_Forum.xlsx, discussion table
(discussion table/name == post table/subject)

ALL_Forum.xlsx, forum table
There about 20 forums predefined-
'Course Announcements, Course Q &amp; A Forum, Social Forum, Post Introduction Video for Video Project, Forum Group Discussion of Topic X, Forum Group Discussion of Topic Y'

Current Limitations due to dropping superfluous table.
* No way to see subscription mapped
* No way to see forum mapped


## Forum viewing notes
Forum viewing can be determined using the events in Logstores.xlsx
eventname	component	action	target	objecttable	objectid	crud
\mod_forum\event\discussion_viewed	mod_forum	viewed	discussion	forum_discussions	2290762	r

Logstores.xlsx, logstore_standard_log
 objecttable=='forum_discussions'
 crud='r'
 timecreated_utc e.g. 2018-10-14 20:26:04
 anonid
 objectid points to discussion.id
 * Can see if student was on campus network or remote
 * Does not capture viewing of specific posts (part of a conversation); but can see when a student views the discussion as a whole.

Event types and their frequency in one course (this is the 'eventname' column)- 
 124 \assignsubmission_onlinetext\event\assessable_uploaded
  59 \assignsubmission_onlinetext\event\submission_created
  65 \assignsubmission_onlinetext\event\submission_updated
  23 \assignsubmission_review\event\submission_created
 519 \assignsubmission_review\event\submission_updated
 602 \block_uiphoto\event\block_viewed
  26 \core\event\badge_listing_viewed
  11 \core\event\calendar_event_created
 459 \core\event\calendar_event_updated
   1 \core\event\course_backup_created
  26 \core\event\course_module_updated
   2 \core\event\course_updated
 171 \core\event\course_user_report_viewed
16451 \core\event\course_viewed
  82 \core\event\grade_deleted
   1 \core\event\group_created
 151 \core\event\group_member_added
  68 \core\event\group_member_removed
   3 \core\event\role_assigned
   2 \core\event\role_unassigned
   1 \core\event\user_enrolment_created
   2 \core\event\user_enrolment_deleted
10505 \core\event\user_graded
 126 \core\event\user_list_viewed
 454 \core\event\user_profile_viewed
  61 \gradereport_grader\event\grade_report_viewed
   3 \gradereport_history\event\grade_report_viewed
   3 \gradereport_outcomes\event\grade_report_viewed
  73 \gradereport_overview\event\grade_report_viewed
 219 \gradereport_singleview\event\grade_report_viewed
4464 \gradereport_user\event\grade_report_viewed
   1 \local_uilinkaggregator\event\sync_ended
 124 \mod_assign\event\assessable_submitted
1148 \mod_assign\event\grading_form_viewed
 628 \mod_assign\event\grading_table_viewed
 237 \mod_assign\event\submission_form_viewed
 768 \mod_assign\event\submission_graded
1869 \mod_assign\event\submission_status_viewed
   3 \mod_assign\event\submission_viewed
 690 \mod_book\event\chapter_viewed
 670 \mod_book\event\course_module_viewed
  58 \mod_choicegroup\event\choice_removed
 148 \mod_choicegroup\event\choice_updated
1099 \mod_choicegroup\event\course_module_viewed
   2 \mod_choicegroup\event\report_viewed
 606 \mod_forum\event\assessable_uploaded
  14 \mod_forum\event\course_module_instance_list_viewed
3391 \mod_forum\event\course_module_viewed
  46 \mod_forum\event\course_searched
  83 \mod_forum\event\discussion_created
  28 \mod_forum\event\discussion_subscription_created
  47 \mod_forum\event\discussion_subscription_deleted
5559 \mod_forum\event\discussion_viewed
 494 \mod_forum\event\post_created
   2 \mod_forum\event\post_deleted
  29 \mod_forum\event\post_updated
  13 \mod_forum\event\subscription_created
   3 \mod_forum\event\subscription_deleted
  23 \mod_forum\event\user_report_viewed
  36 \mod_hsuforum\event\assessable_uploaded
  14 \mod_hsuforum\event\course_module_instance_list_viewed
 176 \mod_hsuforum\event\course_module_viewed
  31 \mod_hsuforum\event\discussion_created
  29 \mod_hsuforum\event\discussion_viewed
   3 \mod_hsuforum\event\post_created
   2 \mod_hsuforum\event\post_updated
   2 \mod_hsuforum\event\subscription_created
   1 \mod_hsuforum\event\subscription_deleted
   1 \mod_hsuforum\event\user_report_viewed
 155 \mod_page\event\course_module_viewed
   1 \mod_quiz\event\attempt_preview_started
2726 \mod_quiz\event\attempt_reviewed
1201 \mod_quiz\event\attempt_started
1204 \mod_quiz\event\attempt_submitted
1404 \mod_quiz\event\attempt_summary_viewed
11309 \mod_quiz\event\attempt_viewed
6480 \mod_quiz\event\course_module_viewed
   4 \mod_quiz\event\edit_page_viewed
  20 \mod_quiz\event\group_override_updated
  34 \mod_quiz\event\report_viewed
  10 \mod_quiz\event\user_override_created
  15 \mod_quiz\event\user_override_updated
1038 \mod_url\event\course_module_viewed
 376 \mod_workshop\event\assessable_uploaded
 273 \mod_workshop\event\assessment_evaluated
4144 \mod_workshop\event\course_module_viewed
   7 \mod_workshop\event\phase_switched
1284 \mod_workshop\event\submission_assessed
 260 \mod_workshop\event\submission_created
 314 \mod_workshop\event\submission_reassessed
 376 \mod_workshop\event\submission_updated
1235 \mod_workshop\event\submission_viewed
  44 \report_log\event\report_viewed
   6 \report_log\event\user_report_viewed
   1 eventname

