/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED
# define YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int base_yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    IDENT = 258,                   /* IDENT  */
    FCONST = 259,                  /* FCONST  */
    SCONST = 260,                  /* SCONST  */
    BCONST = 261,                  /* BCONST  */
    XCONST = 262,                  /* XCONST  */
    Op = 263,                      /* Op  */
    ICONST = 264,                  /* ICONST  */
    PARAM = 265,                   /* PARAM  */
    TYPECAST = 266,                /* TYPECAST  */
    DOT_DOT = 267,                 /* DOT_DOT  */
    COLON_EQUALS = 268,            /* COLON_EQUALS  */
    EQUALS_GREATER = 269,          /* EQUALS_GREATER  */
    INTEGER_DIVISION = 270,        /* INTEGER_DIVISION  */
    POWER_OF = 271,                /* POWER_OF  */
    LAMBDA_ARROW = 272,            /* LAMBDA_ARROW  */
    DOUBLE_ARROW = 273,            /* DOUBLE_ARROW  */
    LESS_EQUALS = 274,             /* LESS_EQUALS  */
    GREATER_EQUALS = 275,          /* GREATER_EQUALS  */
    NOT_EQUALS = 276,              /* NOT_EQUALS  */
    HINT_START = 277,              /* HINT_START  */
    HINT_END = 278,                /* HINT_END  */
    HINT_LPAREN = 279,             /* HINT_LPAREN  */
    HINT_RPAREN = 280,             /* HINT_RPAREN  */
    HINT_COMMA = 281,              /* HINT_COMMA  */
    HINT_INTEGER = 282,            /* HINT_INTEGER  */
    HINT_IDENTIFIER = 283,         /* HINT_IDENTIFIER  */
    HINT_STAR = 284,               /* HINT_STAR  */
    ABORT_P = 285,                 /* ABORT_P  */
    ABSOLUTE_P = 286,              /* ABSOLUTE_P  */
    ACCESS = 287,                  /* ACCESS  */
    ACTION = 288,                  /* ACTION  */
    ADD_P = 289,                   /* ADD_P  */
    ADMIN = 290,                   /* ADMIN  */
    AFTER = 291,                   /* AFTER  */
    AGGREGATE = 292,               /* AGGREGATE  */
    ALL = 293,                     /* ALL  */
    ALSO = 294,                    /* ALSO  */
    ALTER = 295,                   /* ALTER  */
    ALWAYS = 296,                  /* ALWAYS  */
    ANALYSE = 297,                 /* ANALYSE  */
    ANALYZE = 298,                 /* ANALYZE  */
    AND = 299,                     /* AND  */
    ANTI = 300,                    /* ANTI  */
    ANY = 301,                     /* ANY  */
    ARRAY = 302,                   /* ARRAY  */
    AS = 303,                      /* AS  */
    ASC_P = 304,                   /* ASC_P  */
    ASOF = 305,                    /* ASOF  */
    ASSERTION = 306,               /* ASSERTION  */
    ASSIGNMENT = 307,              /* ASSIGNMENT  */
    ASYMMETRIC = 308,              /* ASYMMETRIC  */
    AT = 309,                      /* AT  */
    ATTACH = 310,                  /* ATTACH  */
    ATTRIBUTE = 311,               /* ATTRIBUTE  */
    AUTHORIZATION = 312,           /* AUTHORIZATION  */
    BACKWARD = 313,                /* BACKWARD  */
    BEFORE = 314,                  /* BEFORE  */
    BEGIN_P = 315,                 /* BEGIN_P  */
    BETWEEN = 316,                 /* BETWEEN  */
    BIGINT = 317,                  /* BIGINT  */
    BINARY = 318,                  /* BINARY  */
    BIT = 319,                     /* BIT  */
    BOOLEAN_P = 320,               /* BOOLEAN_P  */
    BOTH = 321,                    /* BOTH  */
    BY = 322,                      /* BY  */
    CACHE = 323,                   /* CACHE  */
    CALL_P = 324,                  /* CALL_P  */
    CALLED = 325,                  /* CALLED  */
    CASCADE = 326,                 /* CASCADE  */
    CASCADED = 327,                /* CASCADED  */
    CASE = 328,                    /* CASE  */
    CAST = 329,                    /* CAST  */
    CATALOG_P = 330,               /* CATALOG_P  */
    CENTURIES_P = 331,             /* CENTURIES_P  */
    CENTURY_P = 332,               /* CENTURY_P  */
    CHAIN = 333,                   /* CHAIN  */
    CHAR_P = 334,                  /* CHAR_P  */
    CHARACTER = 335,               /* CHARACTER  */
    CHARACTERISTICS = 336,         /* CHARACTERISTICS  */
    CHECK_P = 337,                 /* CHECK_P  */
    CHECKPOINT = 338,              /* CHECKPOINT  */
    CLASS = 339,                   /* CLASS  */
    CLOSE = 340,                   /* CLOSE  */
    CLUSTER = 341,                 /* CLUSTER  */
    COALESCE = 342,                /* COALESCE  */
    COLLATE = 343,                 /* COLLATE  */
    COLLATION = 344,               /* COLLATION  */
    COLUMN = 345,                  /* COLUMN  */
    COLUMNS = 346,                 /* COLUMNS  */
    COMMENT = 347,                 /* COMMENT  */
    COMMENTS = 348,                /* COMMENTS  */
    COMMIT = 349,                  /* COMMIT  */
    COMMITTED = 350,               /* COMMITTED  */
    COMPRESSION = 351,             /* COMPRESSION  */
    CONCURRENTLY = 352,            /* CONCURRENTLY  */
    CONFIGURATION = 353,           /* CONFIGURATION  */
    CONFLICT = 354,                /* CONFLICT  */
    CONNECTION = 355,              /* CONNECTION  */
    CONSTRAINT = 356,              /* CONSTRAINT  */
    CONSTRAINTS = 357,             /* CONSTRAINTS  */
    CONTENT_P = 358,               /* CONTENT_P  */
    CONTINUE_P = 359,              /* CONTINUE_P  */
    CONVERSION_P = 360,            /* CONVERSION_P  */
    COPY = 361,                    /* COPY  */
    COST = 362,                    /* COST  */
    CREATE_P = 363,                /* CREATE_P  */
    CROSS = 364,                   /* CROSS  */
    CSV = 365,                     /* CSV  */
    CUBE = 366,                    /* CUBE  */
    CURRENT_P = 367,               /* CURRENT_P  */
    CURSOR = 368,                  /* CURSOR  */
    CYCLE = 369,                   /* CYCLE  */
    DATA_P = 370,                  /* DATA_P  */
    DATABASE = 371,                /* DATABASE  */
    DAY_P = 372,                   /* DAY_P  */
    DAYS_P = 373,                  /* DAYS_P  */
    DEALLOCATE = 374,              /* DEALLOCATE  */
    DEC = 375,                     /* DEC  */
    DECADE_P = 376,                /* DECADE_P  */
    DECADES_P = 377,               /* DECADES_P  */
    DECIMAL_P = 378,               /* DECIMAL_P  */
    DECLARE = 379,                 /* DECLARE  */
    DEFAULT = 380,                 /* DEFAULT  */
    DEFAULTS = 381,                /* DEFAULTS  */
    DEFERRABLE = 382,              /* DEFERRABLE  */
    DEFERRED = 383,                /* DEFERRED  */
    DEFINER = 384,                 /* DEFINER  */
    DELETE_P = 385,                /* DELETE_P  */
    DELIMITER = 386,               /* DELIMITER  */
    DELIMITERS = 387,              /* DELIMITERS  */
    DEPENDS = 388,                 /* DEPENDS  */
    DESC_P = 389,                  /* DESC_P  */
    DESCRIBE = 390,                /* DESCRIBE  */
    DETACH = 391,                  /* DETACH  */
    DICTIONARY = 392,              /* DICTIONARY  */
    DISABLE_P = 393,               /* DISABLE_P  */
    DISCARD = 394,                 /* DISCARD  */
    DISTINCT = 395,                /* DISTINCT  */
    DO = 396,                      /* DO  */
    DOCUMENT_P = 397,              /* DOCUMENT_P  */
    DOMAIN_P = 398,                /* DOMAIN_P  */
    DOUBLE_P = 399,                /* DOUBLE_P  */
    DROP = 400,                    /* DROP  */
    EACH = 401,                    /* EACH  */
    ELSE = 402,                    /* ELSE  */
    ENABLE_P = 403,                /* ENABLE_P  */
    ENCODING = 404,                /* ENCODING  */
    ENCRYPTED = 405,               /* ENCRYPTED  */
    END_P = 406,                   /* END_P  */
    ENUM_P = 407,                  /* ENUM_P  */
    ESCAPE = 408,                  /* ESCAPE  */
    EVENT = 409,                   /* EVENT  */
    EXCEPT = 410,                  /* EXCEPT  */
    EXCLUDE = 411,                 /* EXCLUDE  */
    EXCLUDING = 412,               /* EXCLUDING  */
    EXCLUSIVE = 413,               /* EXCLUSIVE  */
    EXECUTE = 414,                 /* EXECUTE  */
    EXISTS = 415,                  /* EXISTS  */
    EXPLAIN = 416,                 /* EXPLAIN  */
    EXPORT_P = 417,                /* EXPORT_P  */
    EXPORT_STATE = 418,            /* EXPORT_STATE  */
    EXTENSION = 419,               /* EXTENSION  */
    EXTENSIONS = 420,              /* EXTENSIONS  */
    EXTERNAL = 421,                /* EXTERNAL  */
    EXTRACT = 422,                 /* EXTRACT  */
    FALSE_P = 423,                 /* FALSE_P  */
    FAMILY = 424,                  /* FAMILY  */
    FETCH = 425,                   /* FETCH  */
    FILTER = 426,                  /* FILTER  */
    FIRST_P = 427,                 /* FIRST_P  */
    FLOAT_P = 428,                 /* FLOAT_P  */
    FOLLOWING = 429,               /* FOLLOWING  */
    FOR = 430,                     /* FOR  */
    FORCE = 431,                   /* FORCE  */
    FOREIGN = 432,                 /* FOREIGN  */
    FORWARD = 433,                 /* FORWARD  */
    FREEZE = 434,                  /* FREEZE  */
    FROM = 435,                    /* FROM  */
    FULL = 436,                    /* FULL  */
    FUNCTION = 437,                /* FUNCTION  */
    FUNCTIONS = 438,               /* FUNCTIONS  */
    GENERATED = 439,               /* GENERATED  */
    GLOB = 440,                    /* GLOB  */
    GLOBAL = 441,                  /* GLOBAL  */
    GRANT = 442,                   /* GRANT  */
    GRANTED = 443,                 /* GRANTED  */
    GROUP_P = 444,                 /* GROUP_P  */
    GROUPING = 445,                /* GROUPING  */
    GROUPING_ID = 446,             /* GROUPING_ID  */
    GROUPS = 447,                  /* GROUPS  */
    HANDLER = 448,                 /* HANDLER  */
    HAVING = 449,                  /* HAVING  */
    HEADER_P = 450,                /* HEADER_P  */
    HOLD = 451,                    /* HOLD  */
    HOUR_P = 452,                  /* HOUR_P  */
    HOURS_P = 453,                 /* HOURS_P  */
    IDENTITY_P = 454,              /* IDENTITY_P  */
    IF_P = 455,                    /* IF_P  */
    IGNORE_P = 456,                /* IGNORE_P  */
    ILIKE = 457,                   /* ILIKE  */
    IMMEDIATE = 458,               /* IMMEDIATE  */
    IMMUTABLE = 459,               /* IMMUTABLE  */
    IMPLICIT_P = 460,              /* IMPLICIT_P  */
    IMPORT_P = 461,                /* IMPORT_P  */
    IN_P = 462,                    /* IN_P  */
    INCLUDE_P = 463,               /* INCLUDE_P  */
    INCLUDING = 464,               /* INCLUDING  */
    INCREMENT = 465,               /* INCREMENT  */
    INDEX = 466,                   /* INDEX  */
    INDEXES = 467,                 /* INDEXES  */
    INHERIT = 468,                 /* INHERIT  */
    INHERITS = 469,                /* INHERITS  */
    INITIALLY = 470,               /* INITIALLY  */
    INLINE_P = 471,                /* INLINE_P  */
    INNER_P = 472,                 /* INNER_P  */
    INOUT = 473,                   /* INOUT  */
    INPUT_P = 474,                 /* INPUT_P  */
    INSENSITIVE = 475,             /* INSENSITIVE  */
    INSERT = 476,                  /* INSERT  */
    INSTALL = 477,                 /* INSTALL  */
    INSTEAD = 478,                 /* INSTEAD  */
    INT_P = 479,                   /* INT_P  */
    INTEGER = 480,                 /* INTEGER  */
    INTERSECT = 481,               /* INTERSECT  */
    INTERVAL = 482,                /* INTERVAL  */
    INTO = 483,                    /* INTO  */
    INVOKER = 484,                 /* INVOKER  */
    IS = 485,                      /* IS  */
    ISNULL = 486,                  /* ISNULL  */
    ISOLATION = 487,               /* ISOLATION  */
    JOIN = 488,                    /* JOIN  */
    JSON = 489,                    /* JSON  */
    KEY = 490,                     /* KEY  */
    LABEL = 491,                   /* LABEL  */
    LANGUAGE = 492,                /* LANGUAGE  */
    LARGE_P = 493,                 /* LARGE_P  */
    LAST_P = 494,                  /* LAST_P  */
    LATERAL_P = 495,               /* LATERAL_P  */
    LEADING = 496,                 /* LEADING  */
    LEAKPROOF = 497,               /* LEAKPROOF  */
    LEFT = 498,                    /* LEFT  */
    LEVEL = 499,                   /* LEVEL  */
    LIKE = 500,                    /* LIKE  */
    LIMIT = 501,                   /* LIMIT  */
    LISTEN = 502,                  /* LISTEN  */
    LOAD = 503,                    /* LOAD  */
    LOCAL = 504,                   /* LOCAL  */
    LOCATION = 505,                /* LOCATION  */
    LOCK_P = 506,                  /* LOCK_P  */
    LOCKED = 507,                  /* LOCKED  */
    LOGGED = 508,                  /* LOGGED  */
    MACRO = 509,                   /* MACRO  */
    MAP = 510,                     /* MAP  */
    MAPPING = 511,                 /* MAPPING  */
    MATCH = 512,                   /* MATCH  */
    MATERIALIZED = 513,            /* MATERIALIZED  */
    MAXVALUE = 514,                /* MAXVALUE  */
    METHOD = 515,                  /* METHOD  */
    MICROSECOND_P = 516,           /* MICROSECOND_P  */
    MICROSECONDS_P = 517,          /* MICROSECONDS_P  */
    MILLENNIA_P = 518,             /* MILLENNIA_P  */
    MILLENNIUM_P = 519,            /* MILLENNIUM_P  */
    MILLISECOND_P = 520,           /* MILLISECOND_P  */
    MILLISECONDS_P = 521,          /* MILLISECONDS_P  */
    MINUTE_P = 522,                /* MINUTE_P  */
    MINUTES_P = 523,               /* MINUTES_P  */
    MINVALUE = 524,                /* MINVALUE  */
    MODE = 525,                    /* MODE  */
    MONTH_P = 526,                 /* MONTH_P  */
    MONTHS_P = 527,                /* MONTHS_P  */
    MOVE = 528,                    /* MOVE  */
    NAME_P = 529,                  /* NAME_P  */
    NAMES = 530,                   /* NAMES  */
    NATIONAL = 531,                /* NATIONAL  */
    NATURAL = 532,                 /* NATURAL  */
    NCHAR = 533,                   /* NCHAR  */
    NEW = 534,                     /* NEW  */
    NEXT = 535,                    /* NEXT  */
    NO = 536,                      /* NO  */
    NONE = 537,                    /* NONE  */
    NOT = 538,                     /* NOT  */
    NOTHING = 539,                 /* NOTHING  */
    NOTIFY = 540,                  /* NOTIFY  */
    NOTNULL = 541,                 /* NOTNULL  */
    NOWAIT = 542,                  /* NOWAIT  */
    NULL_P = 543,                  /* NULL_P  */
    NULLIF = 544,                  /* NULLIF  */
    NULLS_P = 545,                 /* NULLS_P  */
    NUMERIC = 546,                 /* NUMERIC  */
    OBJECT_P = 547,                /* OBJECT_P  */
    OF = 548,                      /* OF  */
    OFF = 549,                     /* OFF  */
    OFFSET = 550,                  /* OFFSET  */
    OIDS = 551,                    /* OIDS  */
    OLD = 552,                     /* OLD  */
    ON = 553,                      /* ON  */
    ONLY = 554,                    /* ONLY  */
    OPERATOR = 555,                /* OPERATOR  */
    OPTION = 556,                  /* OPTION  */
    OPTIONS = 557,                 /* OPTIONS  */
    OR = 558,                      /* OR  */
    ORDER = 559,                   /* ORDER  */
    ORDINALITY = 560,              /* ORDINALITY  */
    OTHERS = 561,                  /* OTHERS  */
    OUT_P = 562,                   /* OUT_P  */
    OUTER_P = 563,                 /* OUTER_P  */
    OVER = 564,                    /* OVER  */
    OVERLAPS = 565,                /* OVERLAPS  */
    OVERLAY = 566,                 /* OVERLAY  */
    OVERRIDING = 567,              /* OVERRIDING  */
    OWNED = 568,                   /* OWNED  */
    OWNER = 569,                   /* OWNER  */
    PARALLEL = 570,                /* PARALLEL  */
    PARSER = 571,                  /* PARSER  */
    PARTIAL = 572,                 /* PARTIAL  */
    PARTITION = 573,               /* PARTITION  */
    PASSING = 574,                 /* PASSING  */
    PASSWORD = 575,                /* PASSWORD  */
    PERCENT = 576,                 /* PERCENT  */
    PERSISTENT = 577,              /* PERSISTENT  */
    PIVOT = 578,                   /* PIVOT  */
    PIVOT_LONGER = 579,            /* PIVOT_LONGER  */
    PIVOT_WIDER = 580,             /* PIVOT_WIDER  */
    PLACING = 581,                 /* PLACING  */
    PLANS = 582,                   /* PLANS  */
    POLICY = 583,                  /* POLICY  */
    POSITION = 584,                /* POSITION  */
    POSITIONAL = 585,              /* POSITIONAL  */
    PRAGMA_P = 586,                /* PRAGMA_P  */
    PRECEDING = 587,               /* PRECEDING  */
    PRECISION = 588,               /* PRECISION  */
    PREPARE = 589,                 /* PREPARE  */
    PREPARED = 590,                /* PREPARED  */
    PRESERVE = 591,                /* PRESERVE  */
    PRIMARY = 592,                 /* PRIMARY  */
    PRIOR = 593,                   /* PRIOR  */
    PRIVILEGES = 594,              /* PRIVILEGES  */
    PROCEDURAL = 595,              /* PROCEDURAL  */
    PROCEDURE = 596,               /* PROCEDURE  */
    PROGRAM = 597,                 /* PROGRAM  */
    PUBLICATION = 598,             /* PUBLICATION  */
    QUALIFY = 599,                 /* QUALIFY  */
    QUARTER_P = 600,               /* QUARTER_P  */
    QUARTERS_P = 601,              /* QUARTERS_P  */
    QUOTE = 602,                   /* QUOTE  */
    RANGE = 603,                   /* RANGE  */
    READ_P = 604,                  /* READ_P  */
    REAL = 605,                    /* REAL  */
    REASSIGN = 606,                /* REASSIGN  */
    RECHECK = 607,                 /* RECHECK  */
    RECURSIVE = 608,               /* RECURSIVE  */
    REF = 609,                     /* REF  */
    REFERENCES = 610,              /* REFERENCES  */
    REFERENCING = 611,             /* REFERENCING  */
    REFRESH = 612,                 /* REFRESH  */
    REINDEX = 613,                 /* REINDEX  */
    RELATIVE_P = 614,              /* RELATIVE_P  */
    RELEASE = 615,                 /* RELEASE  */
    RENAME = 616,                  /* RENAME  */
    REPEATABLE = 617,              /* REPEATABLE  */
    REPLACE = 618,                 /* REPLACE  */
    REPLICA = 619,                 /* REPLICA  */
    RESET = 620,                   /* RESET  */
    RESPECT_P = 621,               /* RESPECT_P  */
    RESTART = 622,                 /* RESTART  */
    RESTRICT = 623,                /* RESTRICT  */
    RETURNING = 624,               /* RETURNING  */
    RETURNS = 625,                 /* RETURNS  */
    REVOKE = 626,                  /* REVOKE  */
    RIGHT = 627,                   /* RIGHT  */
    ROLE = 628,                    /* ROLE  */
    ROLLBACK = 629,                /* ROLLBACK  */
    ROLLUP = 630,                  /* ROLLUP  */
    ROW = 631,                     /* ROW  */
    ROWS = 632,                    /* ROWS  */
    RULE = 633,                    /* RULE  */
    SAMPLE = 634,                  /* SAMPLE  */
    SAVEPOINT = 635,               /* SAVEPOINT  */
    SCHEMA = 636,                  /* SCHEMA  */
    SCHEMAS = 637,                 /* SCHEMAS  */
    SCOPE = 638,                   /* SCOPE  */
    SCROLL = 639,                  /* SCROLL  */
    SEARCH = 640,                  /* SEARCH  */
    SECOND_P = 641,                /* SECOND_P  */
    SECONDS_P = 642,               /* SECONDS_P  */
    SECRET = 643,                  /* SECRET  */
    SECURITY = 644,                /* SECURITY  */
    SELECT = 645,                  /* SELECT  */
    SEMI = 646,                    /* SEMI  */
    SEQUENCE = 647,                /* SEQUENCE  */
    SEQUENCES = 648,               /* SEQUENCES  */
    SERIALIZABLE = 649,            /* SERIALIZABLE  */
    SERVER = 650,                  /* SERVER  */
    SESSION = 651,                 /* SESSION  */
    SET = 652,                     /* SET  */
    SETOF = 653,                   /* SETOF  */
    SETS = 654,                    /* SETS  */
    SHARE = 655,                   /* SHARE  */
    SHOW = 656,                    /* SHOW  */
    SIMILAR = 657,                 /* SIMILAR  */
    SIMPLE = 658,                  /* SIMPLE  */
    SKIP = 659,                    /* SKIP  */
    SMALLINT = 660,                /* SMALLINT  */
    SNAPSHOT = 661,                /* SNAPSHOT  */
    SOME = 662,                    /* SOME  */
    SQL_P = 663,                   /* SQL_P  */
    STABLE = 664,                  /* STABLE  */
    STANDALONE_P = 665,            /* STANDALONE_P  */
    START = 666,                   /* START  */
    STATEMENT = 667,               /* STATEMENT  */
    STATISTICS = 668,              /* STATISTICS  */
    STDIN = 669,                   /* STDIN  */
    STDOUT = 670,                  /* STDOUT  */
    STORAGE = 671,                 /* STORAGE  */
    STORED = 672,                  /* STORED  */
    STRICT_P = 673,                /* STRICT_P  */
    STRIP_P = 674,                 /* STRIP_P  */
    STRUCT = 675,                  /* STRUCT  */
    SUBSCRIPTION = 676,            /* SUBSCRIPTION  */
    SUBSTRING = 677,               /* SUBSTRING  */
    SUMMARIZE = 678,               /* SUMMARIZE  */
    SYMMETRIC = 679,               /* SYMMETRIC  */
    SYSID = 680,                   /* SYSID  */
    SYSTEM_P = 681,                /* SYSTEM_P  */
    TABLE = 682,                   /* TABLE  */
    TABLES = 683,                  /* TABLES  */
    TABLESAMPLE = 684,             /* TABLESAMPLE  */
    TABLESPACE = 685,              /* TABLESPACE  */
    TEMP = 686,                    /* TEMP  */
    TEMPLATE = 687,                /* TEMPLATE  */
    TEMPORARY = 688,               /* TEMPORARY  */
    TEXT_P = 689,                  /* TEXT_P  */
    THEN = 690,                    /* THEN  */
    TIES = 691,                    /* TIES  */
    TIME = 692,                    /* TIME  */
    TIMESTAMP = 693,               /* TIMESTAMP  */
    TO = 694,                      /* TO  */
    TRAILING = 695,                /* TRAILING  */
    TRANSACTION = 696,             /* TRANSACTION  */
    TRANSFORM = 697,               /* TRANSFORM  */
    TREAT = 698,                   /* TREAT  */
    TRIGGER = 699,                 /* TRIGGER  */
    TRIM = 700,                    /* TRIM  */
    TRUE_P = 701,                  /* TRUE_P  */
    TRUNCATE = 702,                /* TRUNCATE  */
    TRUSTED = 703,                 /* TRUSTED  */
    TRY_CAST = 704,                /* TRY_CAST  */
    TYPE_P = 705,                  /* TYPE_P  */
    TYPES_P = 706,                 /* TYPES_P  */
    UNBOUNDED = 707,               /* UNBOUNDED  */
    UNCOMMITTED = 708,             /* UNCOMMITTED  */
    UNENCRYPTED = 709,             /* UNENCRYPTED  */
    UNION = 710,                   /* UNION  */
    UNIQUE = 711,                  /* UNIQUE  */
    UNKNOWN = 712,                 /* UNKNOWN  */
    UNLISTEN = 713,                /* UNLISTEN  */
    UNLOGGED = 714,                /* UNLOGGED  */
    UNPIVOT = 715,                 /* UNPIVOT  */
    UNTIL = 716,                   /* UNTIL  */
    UPDATE = 717,                  /* UPDATE  */
    USE_P = 718,                   /* USE_P  */
    USER = 719,                    /* USER  */
    USING = 720,                   /* USING  */
    VACUUM = 721,                  /* VACUUM  */
    VALID = 722,                   /* VALID  */
    VALIDATE = 723,                /* VALIDATE  */
    VALIDATOR = 724,               /* VALIDATOR  */
    VALUE_P = 725,                 /* VALUE_P  */
    VALUES = 726,                  /* VALUES  */
    VARCHAR = 727,                 /* VARCHAR  */
    VARIABLE_P = 728,              /* VARIABLE_P  */
    VARIADIC = 729,                /* VARIADIC  */
    VARYING = 730,                 /* VARYING  */
    VERBOSE = 731,                 /* VERBOSE  */
    VERSION_P = 732,               /* VERSION_P  */
    VIEW = 733,                    /* VIEW  */
    VIEWS = 734,                   /* VIEWS  */
    VIRTUAL = 735,                 /* VIRTUAL  */
    VOLATILE = 736,                /* VOLATILE  */
    WEEK_P = 737,                  /* WEEK_P  */
    WEEKS_P = 738,                 /* WEEKS_P  */
    WHEN = 739,                    /* WHEN  */
    WHERE = 740,                   /* WHERE  */
    WHITESPACE_P = 741,            /* WHITESPACE_P  */
    WINDOW = 742,                  /* WINDOW  */
    WITH = 743,                    /* WITH  */
    WITHIN = 744,                  /* WITHIN  */
    WITHOUT = 745,                 /* WITHOUT  */
    WORK = 746,                    /* WORK  */
    WRAPPER = 747,                 /* WRAPPER  */
    WRITE_P = 748,                 /* WRITE_P  */
    XML_P = 749,                   /* XML_P  */
    XMLATTRIBUTES = 750,           /* XMLATTRIBUTES  */
    XMLCONCAT = 751,               /* XMLCONCAT  */
    XMLELEMENT = 752,              /* XMLELEMENT  */
    XMLEXISTS = 753,               /* XMLEXISTS  */
    XMLFOREST = 754,               /* XMLFOREST  */
    XMLNAMESPACES = 755,           /* XMLNAMESPACES  */
    XMLPARSE = 756,                /* XMLPARSE  */
    XMLPI = 757,                   /* XMLPI  */
    XMLROOT = 758,                 /* XMLROOT  */
    XMLSERIALIZE = 759,            /* XMLSERIALIZE  */
    XMLTABLE = 760,                /* XMLTABLE  */
    YEAR_P = 761,                  /* YEAR_P  */
    YEARS_P = 762,                 /* YEARS_P  */
    YES_P = 763,                   /* YES_P  */
    ZONE = 764,                    /* ZONE  */
    NOT_LA = 765,                  /* NOT_LA  */
    NULLS_LA = 766,                /* NULLS_LA  */
    WITH_LA = 767,                 /* WITH_LA  */
    POSTFIXOP = 768,               /* POSTFIXOP  */
    UMINUS = 769                   /* UMINUS  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 14 "third_party/libpg_query/grammar/grammar.y"

	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;
	const char          *conststr;

	char				chr;
	bool				boolean;
	PGJoinType			jtype;
	PGDropBehavior		dbehavior;
	PGOnCommitAction		oncommit;
	PGOnCreateConflict		oncreateconflict;
	PGList				*list;
	PGNode				*node;
	PGValue				*value;
	PGObjectType			objtype;
	PGTypeName			*typnam;
	PGObjectWithArgs		*objwithargs;
	PGDefElem				*defelt;
	PGSortBy				*sortby;
	PGWindowDef			*windef;
	PGJoinExpr			*jexpr;
	PGIndexElem			*ielem;
	PGAlias				*alias;
	PGRangeVar			*range;
	PGIntoClause			*into;
	PGCTEMaterialize			ctematerialize;
	PGWithClause			*with;
	PGInferClause			*infer;
	PGOnConflictClause	*onconflict;
	PGOnConflictActionAlias onconflictshorthand;
	PGAIndices			*aind;
	PGResTarget			*target;
	PGInsertStmt			*istmt;
	PGVariableSetStmt		*vsetstmt;
	PGOverridingKind       override;
	PGSortByDir            sortorder;
	PGSortByNulls          nullorder;
	PGIgnoreNulls          ignorenulls;
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;
	PGInsertColumnOrder bynameorposition;
	PGLoadInstallType loadinstalltype;
	PGTransactionStmtType transactiontype;
	PGHint *hint;
	PGHintElem *hint_elem;

#line 631 "third_party/libpg_query/grammar/grammar_out.hpp"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif




int base_yyparse (core_yyscan_t yyscanner);


#endif /* !YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED  */
