HintStmt:
    HINT_START HintList HINT_END
    {
        PGHint *hint = makeNode(PGHint);
        hint->hints = $2;
        hint->type = T_PGHint;
        $$ = (PGNode *)hint;
    }
    ;

HintList:
    HintElem
    {
        $$ = list_make1($1);
    }
    ;

HintElem:
    HINT_IDENTIFIER HINT_LPAREN HINT_INTEGER HINT_RPAREN
    {
        PGHintElem *elem = makeNode(PGHintElem);
        elem->hint_type = $1;
        elem->value = $3;
        elem->type = T_PGHintElem;
        $$ = (PGNode *)elem;
    }
    ;
