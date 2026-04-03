CREATE TABLE "User" (
    Email TEXT PRIMARY KEY,
    Alias TEXT NOT NULL,
    PairEmail TEXT,
    FOREIGN KEY (PairEmail) REFERENCES "User"(Email)
);

CREATE TABLE VideoSegment (
    ID TEXT PRIMARY KEY,  -- format: video_urlID-ai_mention_timestamp

    video_urlID TEXT,     -- example: 5bzXa6fx57o
    meeting_date DATE,    -- normalized date

    "State" TEXT,
    County TEXT,

    original_row_number INTEGER,
    ai_mention_timestamp TEXT,

    segment_start TEXT,
    segment_end TEXT,
    segment_transcript TEXT
);

CREATE TABLE TCU (
    TCUID TEXT PRIMARY KEY,
    VIDEOID TEXT NOT NULL,
    VIDEOSEGID TEXT NOT NULL,

    tcu_start TEXT,
    tcu_end TEXT,
    tcu_transcript TEXT,
    video_saved BOOLEAN,
    audio_saved BOOLEAN,
    frames_saved BOOLEAN,

    FOREIGN KEY (VIDEOSEGID) REFERENCES VideoSegment(ID)
);


CREATE TABLE Annotation (
    AnnotationID TEXT PRIMARY KEY,

    TCUID TEXT NOT NULL,
    Email TEXT NOT NULL,
    speaker_role TEXT,
    speaker_gender TEXT,
    stance TEXT,
    vocal_tone TEXT,
    facial_expression TEXT,
    coder_notes TEXT,
    annotationtype TEXT CHECK(annotationtype IN ('common', 'irr')) NOT NULL,

    FOREIGN KEY (TCUID) REFERENCES TCU(TCUID),
    FOREIGN KEY (Email) REFERENCES "User"(Email),

    UNIQUE (Email, TCUID)  
);