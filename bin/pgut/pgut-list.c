/*-------------------------------------------------------------------------
 *
 * pgut-list.c : copied from postgres/nodes/list.c
 *
 * Copyright (c) 2009-2023, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "pgut.h"
#include "pgut-list.h"
#include "postgres_fe.h"

#if PG_VERSION_NUM >= 130000
#ifdef USE_VALGRIND
#include <valgrind/memcheck.h>
#else
#define VALGRIND_CHECK_MEM_IS_DEFINED(addr, size)                       do {} while (0)
#define VALGRIND_CREATE_MEMPOOL(context, redzones, zeroed)      do {} while (0)
#define VALGRIND_DESTROY_MEMPOOL(context)                                       do {} while (0)
#define VALGRIND_MAKE_MEM_DEFINED(addr, size)                           do {} while (0)
#define VALGRIND_MAKE_MEM_NOACCESS(addr, size)                          do {} while (0)
#define VALGRIND_MAKE_MEM_UNDEFINED(addr, size)                         do {} while (0)
#define VALGRIND_MEMPOOL_ALLOC(context, addr, size)                     do {} while (0)
#define VALGRIND_MEMPOOL_FREE(context, addr)                            do {} while (0)
#define VALGRIND_MEMPOOL_CHANGE(context, optr, nptr, size)      do {} while (0)
#endif

/* Overhead for the fixed part of a List header, measured in ListCells */
#define LIST_HEADER_OVERHEAD  \
	((int) ((offsetof(List, initial_elements) - 1) / sizeof(ListCell) + 1))
#endif

/*
 * Macros to simplify writing assertions about the type of a list; a
 * NIL list is considered to be an empty list of any type.
 */
#define IsPointerList(l)		((l) == NIL || IsA((l), List))
#if PG_VERSION_NUM >= 130000
#define IsIntegerList(l)		((l) == NIL || IsA((l), IntList))
#define IsOidList(l)			((l) == NIL || IsA((l), OidList))

#ifdef USE_ASSERT_CHECKING
/*
 * Check that the specified List is valid (so far as we can tell).
 */
static void
check_list_invariants(const List *list)
{
	if (list == NIL)
		return;

	Assert(list->length > 0);
	Assert(list->length <= list->max_length);
	Assert(list->elements != NULL);

	Assert(list->type == T_List ||
		   list->type == T_IntList ||
		   list->type == T_OidList);
}
#else
#define check_list_invariants(l)  ((void) 0)
#endif							/* USE_ASSERT_CHECKING */

/*
 * Return a freshly allocated List with room for at least min_size cells.
 *
 * Since empty non-NIL lists are invalid, new_list() sets the initial length
 * to min_size, effectively marking that number of cells as valid; the caller
 * is responsible for filling in their data.
 */
static List *
new_list(NodeTag type, int min_size)
{
	List	   *newlist;
	int			max_size;

	Assert(min_size > 0);

	/*
	 * We allocate all the requested cells, and possibly some more, as part of
	 * the same palloc request as the List header.  This is a big win for the
	 * typical case of short fixed-length lists.  It can lose if we allocate a
	 * moderately long list and then it gets extended; we'll be wasting more
	 * initial_elements[] space than if we'd made the header small.  However,
	 * rounding up the request as we do in the normal code path provides some
	 * defense against small extensions.
	 */

	/*
	 * Normally, we set up a list with some extra cells, to allow it to grow
	 * without a repalloc.  Prefer cell counts chosen to make the total
	 * allocation a power-of-2, since palloc would round it up to that anyway.
	 * (That stops being true for very large allocations, but very long lists
	 * are infrequent, so it doesn't seem worth special logic for such cases.)
	 *
	 * The minimum allocation is 8 ListCell units, providing either 4 or 5
	 * available ListCells depending on the machine's word width.  Counting
	 * palloc's overhead, this uses the same amount of space as a one-cell
	 * list did in the old implementation, and less space for any longer list.
	 *
	 * We needn't worry about integer overflow; no caller passes min_size
	 * that's more than twice the size of an existing list, so the size limits
	 * within palloc will ensure that we don't overflow here.
	 */
 	max_size = 8;
 	while (max_size < min_size + LIST_HEADER_OVERHEAD)
 		max_size *= 2;

	max_size -= LIST_HEADER_OVERHEAD;

	newlist = (List *) palloc(offsetof(List, initial_elements) +
							  max_size * sizeof(ListCell));
	newlist->type = type;
	newlist->length = min_size;
	newlist->max_length = max_size;
	newlist->elements = newlist->initial_elements;

	return newlist;
}

/*
 * Enlarge an existing non-NIL List to have room for at least min_size cells.
 *
 * This does *not* update list->length, as some callers would find that
 * inconvenient.  (list->length had better be the correct number of existing
 * valid cells, though.)
 */
static void
enlarge_list(List *list, int min_size)
{
	int			new_max_len;

	Assert(min_size > list->max_length);	/* else we shouldn't be here */

	/*
	 * As above, we prefer power-of-two total allocations; but here we need
	 * not account for list header overhead.
	 */

	/* clamp the minimum value to 16, a semi-arbitrary small power of 2 */
 	new_max_len = 16;
 	while (new_max_len < min_size)
 		new_max_len *= 2;

	if (list->elements == list->initial_elements)
	{
		/*
		 * Replace original in-line allocation with a separate palloc block.
		 * Ensure it is in the same memory context as the List header.  (The
		 * previous List implementation did not offer any guarantees about
		 * keeping all list cells in the same context, but it seems reasonable
		 * to create such a guarantee now.)
		 */
		list->elements = (ListCell *)
 			pgut_newarray(ListCell, new_max_len * sizeof(ListCell));
		memcpy(list->elements, list->initial_elements,
			   list->length * sizeof(ListCell));

		/*
		 * We must not move the list header, so it's unsafe to try to reclaim
		 * the initial_elements[] space via repalloc.  In debugging builds,
		 * however, we can clear that space and/or mark it inaccessible.
		 * (wipe_mem includes VALGRIND_MAKE_MEM_NOACCESS.)
		 */
		VALGRIND_MAKE_MEM_NOACCESS(list->initial_elements,
								   list->max_length * sizeof(ListCell));
	}
	else
	{
		/* Normally, let repalloc deal with enlargement */
		list->elements = (ListCell *) repalloc(list->elements,
											   new_max_len * sizeof(ListCell));
	}

	list->max_length = new_max_len;
}

/*
 * Convenience functions to construct short Lists from given values.
 * (These are normally invoked via the list_makeN macros.)
 */
List *
list_make1_impl(NodeTag t, ListCell datum1)
{
	List	   *list = new_list(t, 1);

	list->elements[0] = datum1;
	check_list_invariants(list);
	return list;
}

List *
list_make2_impl(NodeTag t, ListCell datum1, ListCell datum2)
{
	List	   *list = new_list(t, 2);

	list->elements[0] = datum1;
	list->elements[1] = datum2;
	check_list_invariants(list);
	return list;
}

List *
list_make3_impl(NodeTag t, ListCell datum1, ListCell datum2,
				ListCell datum3)
{
	List	   *list = new_list(t, 3);

	list->elements[0] = datum1;
	list->elements[1] = datum2;
	list->elements[2] = datum3;
	check_list_invariants(list);
	return list;
}

List *
list_make4_impl(NodeTag t, ListCell datum1, ListCell datum2,
				ListCell datum3, ListCell datum4)
{
	List	   *list = new_list(t, 4);

	list->elements[0] = datum1;
	list->elements[1] = datum2;
	list->elements[2] = datum3;
	list->elements[3] = datum4;
	check_list_invariants(list);
	return list;
}
#else /* PG_VERSION under 13 */
/*
 * Return a freshly allocated List. Since empty non-NIL lists are
 * invalid, new_list() also allocates the head cell of the new list:
 * the caller should be sure to fill in that cell's data.
 */
static List *
new_list(NodeTag type)
{
	List	   *new_list;
	ListCell   *new_head;

	new_head = pgut_new(ListCell);
	new_head->next = NULL;
	/* new_head->data is left undefined! */

	new_list = pgut_new(List);
	new_list->type = type;
	new_list->length = 1;
	new_list->head = new_head;
	new_list->tail = new_head;

	return new_list;
}
#endif

/*
 * Make room for a new head cell in the given (non-NIL) list.
 *
 * The data in the new head cell is undefined; the caller should be
 * sure to fill it in
 */
static void
new_head_cell(List *list)
{
#if PG_VERSION_NUM >= 130000	
	/* Enlarge array if necessary */
	if (list->length >= list->max_length)
		enlarge_list(list, list->length + 1);
	/* Now shove the existing data over */
	memmove(&list->elements[1], &list->elements[0],
			list->length * sizeof(ListCell));
#else
	ListCell   *new_head;

	new_head = pgut_new(ListCell);
	new_head->next = list->head;

	list->head = new_head;
#endif				
	list->length++;
}

/*
 * Make room for a new tail cell in the given (non-NIL) list.
 *
 * The data in the new tail cell is undefined; the caller should be
 * sure to fill it in
 */
static void
new_tail_cell(List *list)
{
#if PG_VERSION_NUM >= 130000	
	/* Enlarge array if necessary */
	if (list->length >= list->max_length)
		enlarge_list(list, list->length + 1);
#else
	ListCell   *new_tail;

	new_tail = pgut_new(ListCell);
	new_tail->next = NULL;

	list->tail->next = new_tail;
	list->tail = new_tail;
#endif			
	list->length++;
}

/*
 * Append a pointer to the list. A pointer to the modified list is
 * returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * first argument.
 */
List *
lappend(List *list, void *datum)
{
	Assert(IsPointerList(list));

	if (list == NIL)
#if PG_VERSION_NUM >= 130000	
		list = new_list(T_List, 1);
#else
		list = new_list(T_List);
#endif				
	else
		new_tail_cell(list);

#if PG_VERSION_NUM >= 130000
	lfirst(list_tail(list)) = datum;
	check_list_invariants(list);
#else
	lfirst(list->tail) = datum;
#endif		
	return list;
}

#if PG_VERSION_NUM >= 130000
/*
 * Append an integer to the specified list. See lappend()
 */
List *
lappend_int(List *list, int datum)
{
	Assert(IsIntegerList(list));

	if (list == NIL)
		list = new_list(T_IntList, 1);
	else
		new_tail_cell(list);

	lfirst_int(list_tail(list)) = datum;
	check_list_invariants(list);
	return list;
}

/*
 * Append an OID to the specified list. See lappend()
 */
List *
lappend_oid(List *list, Oid datum)
{
	Assert(IsOidList(list));

	if (list == NIL)
		list = new_list(T_OidList, 1);
	else
		new_tail_cell(list);

	lfirst_oid(list_tail(list)) = datum;
	check_list_invariants(list);
	return list;
}

/*
 * Make room for a new cell at position 'pos' (measured from 0).
 * The data in the cell is left undefined, and must be filled in by the
 * caller. 'list' is assumed to be non-NIL, and 'pos' must be a valid
 * list position, ie, 0 <= pos <= list's length.
 * Returns address of the new cell.
 */
static ListCell *
insert_new_cell(List *list, int pos)
{
	Assert(pos >= 0 && pos <= list->length);

	/* Enlarge array if necessary */
	if (list->length >= list->max_length)
		enlarge_list(list, list->length + 1);
	/* Now shove the existing data over */
	if (pos < list->length)
		memmove(&list->elements[pos + 1], &list->elements[pos],
				(list->length - pos) * sizeof(ListCell));
	list->length++;

	return &list->elements[pos];
}

/*
 * Insert the given datum at position 'pos' (measured from 0) in the list.
 * 'pos' must be valid, ie, 0 <= pos <= list's length.
 */
List *
list_insert_nth(List *list, int pos, void *datum)
{
	if (list == NIL)
	{
		Assert(pos == 0);
		return list_make1(datum);
	}
	Assert(IsPointerList(list));
	lfirst(insert_new_cell(list, pos)) = datum;
	check_list_invariants(list);
	return list;
}

List *
list_insert_nth_int(List *list, int pos, int datum)
{
	if (list == NIL)
	{
		Assert(pos == 0);
		return list_make1_int(datum);
	}
	Assert(IsIntegerList(list));
	lfirst_int(insert_new_cell(list, pos)) = datum;
	check_list_invariants(list);
	return list;
}

List *
list_insert_nth_oid(List *list, int pos, Oid datum)
{
	if (list == NIL)
	{
		Assert(pos == 0);
		return list_make1_oid(datum);
	}
	Assert(IsOidList(list));
	lfirst_oid(insert_new_cell(list, pos)) = datum;
	check_list_invariants(list);
	return list;
}
#else  /* PG_VERSION under 13 */
/*
 * Add a new cell to the list, in the position after 'prev_cell'. The
 * data in the cell is left undefined, and must be filled in by the
 * caller. 'list' is assumed to be non-NIL, and 'prev_cell' is assumed
 * to be non-NULL and a member of 'list'.
 */
static ListCell *
add_new_cell(List *list, ListCell *prev_cell)
{
	ListCell   *new_cell;

	new_cell = pgut_new(ListCell);
	/* new_cell->data is left undefined! */
	new_cell->next = prev_cell->next;
	prev_cell->next = new_cell;

	if (list->tail == prev_cell)
		list->tail = new_cell;

	list->length++;

	return new_cell;
}

/*
 * Add a new cell to the specified list (which must be non-NIL);
 * it will be placed after the list cell 'prev' (which must be
 * non-NULL and a member of 'list'). The data placed in the new cell
 * is 'datum'. The newly-constructed cell is returned.
 */
ListCell *
lappend_cell(List *list, ListCell *prev, void *datum)
{
	ListCell   *new_cell;

	Assert(IsPointerList(list));

	new_cell = add_new_cell(list, prev);
	lfirst(new_cell) = datum;
	return new_cell;
}
#endif

/*
 * Prepend a new element to the list. A pointer to the modified list
 * is returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * second argument.
 *
 * Caution: before Postgres 8.0, the original List was unmodified and
 * could be considered to retain its separate identity.  This is no longer
 * the case.
 */
List *
lcons(void *datum, List *list)
{
	Assert(IsPointerList(list));

	if (list == NIL)
#if PG_VERSION_NUM >= 130000
		list = new_list(T_List, 1);
#else
		list = new_list(T_List);
#endif				
	else
		new_head_cell(list);

#if PG_VERSION_NUM >= 130000
	lfirst(list_head(list)) = datum;
	check_list_invariants(list);
#else
	lfirst(list->head) = datum;
#endif	
	return list;
}

#if PG_VERSION_NUM >= 130000
/*
 * Prepend an integer to the list. See lcons()
 */
List *
lcons_int(int datum, List *list)
{
	Assert(IsIntegerList(list));

	if (list == NIL)
		list = new_list(T_IntList, 1);
	else
		new_head_cell(list);

	lfirst_int(list_head(list)) = datum;
	check_list_invariants(list);
	return list;
}

/*
 * Prepend an OID to the list. See lcons()
 */
List *
lcons_oid(Oid datum, List *list)
{
	Assert(IsOidList(list));

	if (list == NIL)
		list = new_list(T_OidList, 1);
	else
		new_head_cell(list);

	lfirst_oid(list_head(list)) = datum;
	check_list_invariants(list);
	return list;
}

/*
 * Concatenate list2 to the end of list1, and return list1.
 *
 * This is equivalent to lappend'ing each element of list2, in order, to list1.
 * list1 is destructively changed, list2 is not.  (However, in the case of
 * pointer lists, list1 and list2 will point to the same structures.)
 *
 * Callers should be sure to use the return value as the new pointer to the
 * concatenated list: the 'list1' input pointer may or may not be the same
 * as the returned pointer.
 */
List *
list_concat(List *list1, const List *list2)
{
	int			new_len;

	if (list1 == NIL)
		return list_copy(list2);
	if (list2 == NIL)
		return list1;

	Assert(list1->type == list2->type);

	new_len = list1->length + list2->length;
	/* Enlarge array if necessary */
	if (new_len > list1->max_length)
		enlarge_list(list1, new_len);

	/* Even if list1 == list2, using memcpy should be safe here */
	memcpy(&list1->elements[list1->length], &list2->elements[0],
		   list2->length * sizeof(ListCell));
	list1->length = new_len;

	check_list_invariants(list1);
	return list1;
}

/*
 * Form a new list by concatenating the elements of list1 and list2.
 *
 * Neither input list is modified.  (However, if they are pointer lists,
 * the output list will point to the same structures.)
 *
 * This is equivalent to, but more efficient than,
 * list_concat(list_copy(list1), list2).
 * Note that some pre-v13 code might list_copy list2 as well, but that's
 * pointless now.
 */
List *
list_concat_copy(const List *list1, const List *list2)
{
	List	   *result;
	int			new_len;

	if (list1 == NIL)
		return list_copy(list2);
	if (list2 == NIL)
		return list_copy(list1);

	Assert(list1->type == list2->type);

	new_len = list1->length + list2->length;
	result = new_list(list1->type, new_len);
	memcpy(result->elements, list1->elements,
		   list1->length * sizeof(ListCell));
	memcpy(result->elements + list1->length, list2->elements,
		   list2->length * sizeof(ListCell));

	check_list_invariants(result);
	return result;
}
#else /* PG_VERSION under 13 */
/*
 * Concatenate list2 to the end of list1, and return list1. list1 is
 * destructively changed. Callers should be sure to use the return
 * value as the new pointer to the concatenated list: the 'list1'
 * input pointer may or may not be the same as the returned pointer.
 *
 * The nodes in list2 are merely appended to the end of list1 in-place
 * (i.e. they aren't copied), and the list2 handle is free-ed. This is
 * an incopatible change from backend codes.
 */
List *
list_concat(List *list1, List *list2)
{
	if (list1 == NIL)
		return list2;
	if (list2 == NIL)
		return list1;
	if (list1 == list2)
		return list1;

	Assert(list1->type == list2->type);

	list1->length += list2->length;
	list1->tail->next = list2->head;
	list1->tail = list2->tail;

	/* Note: free list2 handle but keep items in it */
	free(list2);

	return list1;
}
#endif

/*
 * Truncate 'list' to contain no more than 'new_size' elements. This
 * modifies the list in-place! Despite this, callers should use the
 * pointer returned by this function to refer to the newly truncated
 * list -- it may or may not be the same as the pointer that was
 * passed.
 *
 * Note that any cells removed by list_truncate() are NOT pfree'd.
 */
List *
list_truncate(List *list, int new_size)
{
#if PG_VERSION_NUM < 130000
	ListCell   *cell;
	int			n;
#endif
	if (new_size <= 0)
		return NIL;				/* truncate to zero length */

	/* If asked to effectively extend the list, do nothing */
#if PG_VERSION_NUM >= 130000	
	if (new_size < list_length(list))
		list->length = new_size;

	/*
	 * Note: unlike the individual-list-cell deletion functions, we don't move
	 * the list cells to new storage, even in DEBUG_LIST_MEMORY_USAGE mode.
	 * This is because none of them can move in this operation, so just like
	 * in the old cons-cell-based implementation, this function doesn't
	 * invalidate any pointers to cells of the list.  This is also the reason
	 * for not wiping the memory of the deleted cells: the old code didn't
	 * free them either.  Perhaps later we'll tighten this up.
	 */
#else
	if (new_size >= list_length(list))
		return list;

	n = 1;
	foreach(cell, list)
	{
		if (n == new_size)
		{
			cell->next = NULL;
			list->tail = cell;
			list->length = new_size;
			return list;
		}
		n++;
	}

	/* keep the compiler quiet; never reached */
	Assert(false);
#endif

	return list;
}

#if PG_VERSION_NUM < 90200
/*
 * Locate the n'th cell (counting from 0) of the list.  It is an assertion
 * failure if there is no such cell.
 */
static ListCell *
list_nth_cell(List *list, int n)
{
	ListCell   *match;

	Assert(list != NIL);
	Assert(n >= 0);
	Assert(n < list->length);

	/* Does the caller actually mean to fetch the tail? */
	if (n == list->length - 1)
		return list->tail;

	for (match = list->head; n-- > 0; match = match->next)
		;

	return match;
}

/*
 * Return the data value contained in the n'th element of the
 * specified list. (List elements begin at 0.)
 */
void *
list_nth(List *list, int n)
{
	Assert(IsPointerList(list));
	return lfirst(list_nth_cell(list, n));
}
#endif

#if PG_VERSION_NUM < 90200
/*
 * Return true iff 'datum' is a member of the list. Equality is
 * determined by using simple pointer comparison.
 */
bool
list_member_ptr(List *list, void *datum)
{
	ListCell   *cell;

	Assert(IsPointerList(list));

	foreach(cell, list)
	{
		if (lfirst(cell) == datum)
			return true;
	}

	return false;
}

#elif PG_VERSION_NUM >= 130000
/*
 * Return true iff 'datum' is a member of the list. Equality is
 * determined via equal(), so callers should ensure that they pass a
 * Node as 'datum'.
 */
bool
list_member(const List *list, const void *datum)
{
	const ListCell *cell;

	Assert(IsPointerList(list));
	check_list_invariants(list);

	foreach(cell, list)
	{
		if (lfirst(cell) == datum)
			return true;
	}

	return false;
}

/*
 * Return true iff 'datum' is a member of the list. Equality is
 * determined by using simple pointer comparison.
 */
bool
list_member_ptr(const List *list, const void *datum)
{
	const ListCell *cell;

	Assert(IsPointerList(list));
	check_list_invariants(list);

	foreach(cell, list)
	{
		if (lfirst(cell) == datum)
			return true;
	}

	return false;
}

/*
 * Return true iff the integer 'datum' is a member of the list.
 */
bool
list_member_int(const List *list, int datum)
{
	const ListCell *cell;

	Assert(IsIntegerList(list));
	check_list_invariants(list);

	foreach(cell, list)
	{
		if (lfirst_int(cell) == datum)
			return true;
	}

	return false;
}

/*
 * Return true iff the OID 'datum' is a member of the list.
 */
bool
list_member_oid(const List *list, Oid datum)
{
	const ListCell *cell;

	Assert(IsOidList(list));
	check_list_invariants(list);

	foreach(cell, list)
	{
		if (lfirst_oid(cell) == datum)
			return true;
	}

	return false;
}
#endif

#if PG_VERSION_NUM >= 130000
/*
 * Delete the n'th cell (counting from 0) in list.
 *
 * The List is pfree'd if this was the last member.
 */
List *
list_delete_nth_cell(List *list, int n)
{
	check_list_invariants(list);

	Assert(n >= 0 && n < list->length);

	/*
	 * If we're about to delete the last node from the list, free the whole
	 * list instead and return NIL, which is the only valid representation of
	 * a zero-length list.
	 */
	if (list->length == 1)
	{
		list_free(list);
		return NIL;
	}

	/*
	 * Otherwise, we normally just collapse out the removed element.  But for
	 * debugging purposes, move the whole list contents someplace else.
	 *
	 * (Note that we *must* keep the contents in the same memory context.)
	 */
	memmove(&list->elements[n], &list->elements[n + 1],
			(list->length - 1 - n) * sizeof(ListCell));
	list->length--;
	
	return list;
}

/*
 * Delete 'cell' from 'list'.
 *
 * The List is pfree'd if this was the last member.  However, we do not
 * touch any data the cell might've been pointing to.
 */
List *
list_delete_cell(List *list, ListCell *cell)
{
	return list_delete_nth_cell(list, cell - list->elements);
}

/*
 * Delete the first cell in list that matches datum, if any.
 * Equality is determined via equal().
 */
List *
list_delete(List *list, void *datum)
{
	ListCell   *cell;

	Assert(IsPointerList(list));
	check_list_invariants(list);

	foreach(cell, list)
	{
		if (lfirst(cell) == datum)
			return list_delete_cell(list, cell);
	}

	/* Didn't find a match: return the list unmodified */
	return list;
}
#else  /* PG_VERSION under 13 */
/*
 * Delete 'cell' from 'list'; 'prev' is the previous element to 'cell'
 * in 'list', if any (i.e. prev == NULL iff list->head == cell)
 *
 * The cell is free'd, as is the List header if this was the last member.
 */
List *
list_delete_cell(List *list, ListCell *cell, ListCell *prev)
{
	Assert(prev != NULL ? lnext(prev) == cell : list_head(list) == cell);

	/*
	 * If we're about to delete the last node from the list, free the whole
	 * list instead and return NIL, which is the only valid representation of
	 * a zero-length list.
	 */
	if (list->length == 1)
	{
		list_free(list);
		return NIL;
	}

	/*
	 * Otherwise, adjust the necessary list links, deallocate the particular
	 * node we have just removed, and return the list we were given.
	 */
	list->length--;

	if (prev)
		prev->next = cell->next;
	else
		list->head = cell->next;

	if (list->tail == cell)
		list->tail = prev;

	free(cell);
	return list;
}
#endif

/* As above, but use simple pointer equality */
List *
list_delete_ptr(List *list, void *datum)
{
	ListCell   *cell;
#if PG_VERSION_NUM < 130000
	ListCell   *prev;
#endif		

	Assert(IsPointerList(list));
#if PG_VERSION_NUM >= 130000	
	check_list_invariants(list);
#else
	prev = NULL;
#endif	

	foreach(cell, list)
	{
		if (lfirst(cell) == datum)
#if PG_VERSION_NUM >= 130000			
			return list_delete_cell(list, cell);
#else
			return list_delete_cell(list, cell, prev);
#endif						
	}

	/* Didn't find a match: return the list unmodified */
	return list;
}

#if PG_VERSION_NUM >= 130000
/* As above, but for integers */
List *
list_delete_int(List *list, int datum)
{
	ListCell   *cell;

	Assert(IsIntegerList(list));
	check_list_invariants(list);

	foreach(cell, list)
	{
		if (lfirst_int(cell) == datum)
			return list_delete_cell(list, cell);
	}

	/* Didn't find a match: return the list unmodified */
	return list;
}

/* As above, but for OIDs */
List *
list_delete_oid(List *list, Oid datum)
{
	ListCell   *cell;

	Assert(IsOidList(list));
	check_list_invariants(list);

	foreach(cell, list)
	{
		if (lfirst_oid(cell) == datum)
			return list_delete_cell(list, cell);
	}

	/* Didn't find a match: return the list unmodified */
	return list;
}
#endif

/*
 * Delete the first element of the list.
 *
 * This is useful to replace the Lisp-y code "list = lnext(list);" in cases
 * where the intent is to alter the list rather than just traverse it.
 * Beware that the list is modified, whereas the Lisp-y coding leaves
 * the original list head intact in case there's another pointer to it.
 */
List *
list_delete_first(List *list)
{
#if PG_VERSION_NUM >= 130000	
	check_list_invariants(list);
#endif

	if (list == NIL)
		return NIL;				/* would an error be better? */

#if PG_VERSION_NUM >= 130000
	return list_delete_nth_cell(list, 0);
#else
	return list_delete_cell(list, list_head(list), NULL);
#endif		
}

#if PG_VERSION_NUM >= 130000
/*
 * Delete the last element of the list.
 *
 * This is the opposite of list_delete_first(), but is noticeably cheaper
 * with a long list, since no data need be moved.
 */
List *
list_delete_last(List *list)
{
	check_list_invariants(list);

	if (list == NIL)
		return NIL;				/* would an error be better? */

	/* list_truncate won't free list if it goes to empty, but this should */
	if (list_length(list) <= 1)
	{
		list_free(list);
		return NIL;
	}

	return list_truncate(list, list_length(list) - 1);
}
#endif

#if PG_VERSION_NUM >= 130000
/*
 * Generate the union of two lists. This is calculated by copying
 * list1 via list_copy(), then adding to it all the members of list2
 * that aren't already in list1.
 *
 * Whether an element is already a member of the list is determined
 * via equal().
 *
 * The returned list is newly-allocated, although the content of the
 * cells is the same (i.e. any pointed-to objects are not copied).
 *
 * NB: this function will NOT remove any duplicates that are present
 * in list1 (so it only performs a "union" if list1 is known unique to
 * start with).  Also, if you are about to write "x = list_union(x, y)"
 * you probably want to use list_concat_unique() instead to avoid wasting
 * the storage of the old x list.
 *
 * This function could probably be implemented a lot faster if it is a
 * performance bottleneck.
 */
List *
list_union(const List *list1, const List *list2)
{
	List	   *result;
	const ListCell *cell;

	Assert(IsPointerList(list1));
	Assert(IsPointerList(list2));

	result = list_copy(list1);
	foreach(cell, list2)
	{
		if (!list_member(result, lfirst(cell)))
			result = lappend(result, lfirst(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * This variant of list_union() determines duplicates via simple
 * pointer comparison.
 */
List *
list_union_ptr(const List *list1, const List *list2)
{
	List	   *result;
	const ListCell *cell;

	Assert(IsPointerList(list1));
	Assert(IsPointerList(list2));

	result = list_copy(list1);
	foreach(cell, list2)
	{
		if (!list_member_ptr(result, lfirst(cell)))
			result = lappend(result, lfirst(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * This variant of list_union() operates upon lists of integers.
 */
List *
list_union_int(const List *list1, const List *list2)
{
	List	   *result;
	const ListCell *cell;

	Assert(IsIntegerList(list1));
	Assert(IsIntegerList(list2));

	result = list_copy(list1);
	foreach(cell, list2)
	{
		if (!list_member_int(result, lfirst_int(cell)))
			result = lappend_int(result, lfirst_int(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * This variant of list_union() operates upon lists of OIDs.
 */
List *
list_union_oid(const List *list1, const List *list2)
{
	List	   *result;
	const ListCell *cell;

	Assert(IsOidList(list1));
	Assert(IsOidList(list2));

	result = list_copy(list1);
	foreach(cell, list2)
	{
		if (!list_member_oid(result, lfirst_oid(cell)))
			result = lappend_oid(result, lfirst_oid(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * Return a list that contains all the cells that are in both list1 and
 * list2.  The returned list is freshly allocated via palloc(), but the
 * cells themselves point to the same objects as the cells of the
 * input lists.
 *
 * Duplicate entries in list1 will not be suppressed, so it's only a true
 * "intersection" if list1 is known unique beforehand.
 *
 * This variant works on lists of pointers, and determines list
 * membership via equal().  Note that the list1 member will be pointed
 * to in the result.
 */
List *
list_intersection(const List *list1, const List *list2)
{
	List	   *result;
	const ListCell *cell;

	if (list1 == NIL || list2 == NIL)
		return NIL;

	Assert(IsPointerList(list1));
	Assert(IsPointerList(list2));

	result = NIL;
	foreach(cell, list1)
	{
		if (list_member(list2, lfirst(cell)))
			result = lappend(result, lfirst(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * As list_intersection but operates on lists of integers.
 */
List *
list_intersection_int(const List *list1, const List *list2)
{
	List	   *result;
	const ListCell *cell;

	if (list1 == NIL || list2 == NIL)
		return NIL;

	Assert(IsIntegerList(list1));
	Assert(IsIntegerList(list2));

	result = NIL;
	foreach(cell, list1)
	{
		if (list_member_int(list2, lfirst_int(cell)))
			result = lappend_int(result, lfirst_int(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * Return a list that contains all the cells in list1 that are not in
 * list2. The returned list is freshly allocated via palloc(), but the
 * cells themselves point to the same objects as the cells of the
 * input lists.
 *
 * This variant works on lists of pointers, and determines list
 * membership via equal()
 */
List *
list_difference(const List *list1, const List *list2)
{
	const ListCell *cell;
	List	   *result = NIL;

	Assert(IsPointerList(list1));
	Assert(IsPointerList(list2));

	if (list2 == NIL)
		return list_copy(list1);

	foreach(cell, list1)
	{
		if (!list_member(list2, lfirst(cell)))
			result = lappend(result, lfirst(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * This variant of list_difference() determines list membership via
 * simple pointer equality.
 */
List *
list_difference_ptr(const List *list1, const List *list2)
{
	const ListCell *cell;
	List	   *result = NIL;

	Assert(IsPointerList(list1));
	Assert(IsPointerList(list2));

	if (list2 == NIL)
		return list_copy(list1);

	foreach(cell, list1)
	{
		if (!list_member_ptr(list2, lfirst(cell)))
			result = lappend(result, lfirst(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * This variant of list_difference() operates upon lists of integers.
 */
List *
list_difference_int(const List *list1, const List *list2)
{
	const ListCell *cell;
	List	   *result = NIL;

	Assert(IsIntegerList(list1));
	Assert(IsIntegerList(list2));

	if (list2 == NIL)
		return list_copy(list1);

	foreach(cell, list1)
	{
		if (!list_member_int(list2, lfirst_int(cell)))
			result = lappend_int(result, lfirst_int(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * This variant of list_difference() operates upon lists of OIDs.
 */
List *
list_difference_oid(const List *list1, const List *list2)
{
	const ListCell *cell;
	List	   *result = NIL;

	Assert(IsOidList(list1));
	Assert(IsOidList(list2));

	if (list2 == NIL)
		return list_copy(list1);

	foreach(cell, list1)
	{
		if (!list_member_oid(list2, lfirst_oid(cell)))
			result = lappend_oid(result, lfirst_oid(cell));
	}

	check_list_invariants(result);
	return result;
}

/*
 * Append datum to list, but only if it isn't already in the list.
 *
 * Whether an element is already a member of the list is determined
 * via equal().
 */
List *
list_append_unique(List *list, void *datum)
{
	if (list_member(list, datum))
		return list;
	else
		return lappend(list, datum);
}

/*
 * This variant of list_append_unique() determines list membership via
 * simple pointer equality.
 */
List *
list_append_unique_ptr(List *list, void *datum)
{
	if (list_member_ptr(list, datum))
		return list;
	else
		return lappend(list, datum);
}

/*
 * This variant of list_append_unique() operates upon lists of integers.
 */
List *
list_append_unique_int(List *list, int datum)
{
	if (list_member_int(list, datum))
		return list;
	else
		return lappend_int(list, datum);
}

/*
 * This variant of list_append_unique() operates upon lists of OIDs.
 */
List *
list_append_unique_oid(List *list, Oid datum)
{
	if (list_member_oid(list, datum))
		return list;
	else
		return lappend_oid(list, datum);
}

/*
 * Append to list1 each member of list2 that isn't already in list1.
 *
 * Whether an element is already a member of the list is determined
 * via equal().
 *
 * This is almost the same functionality as list_union(), but list1 is
 * modified in-place rather than being copied. However, callers of this
 * function may have strict ordering expectations -- i.e. that the relative
 * order of those list2 elements that are not duplicates is preserved.
 */
List *
list_concat_unique(List *list1, const List *list2)
{
	ListCell   *cell;

	Assert(IsPointerList(list1));
	Assert(IsPointerList(list2));

	foreach(cell, list2)
	{
		if (!list_member(list1, lfirst(cell)))
			list1 = lappend(list1, lfirst(cell));
	}

	check_list_invariants(list1);
	return list1;
}

/*
 * This variant of list_concat_unique() determines list membership via
 * simple pointer equality.
 */
List *
list_concat_unique_ptr(List *list1, const List *list2)
{
	ListCell   *cell;

	Assert(IsPointerList(list1));
	Assert(IsPointerList(list2));

	foreach(cell, list2)
	{
		if (!list_member_ptr(list1, lfirst(cell)))
			list1 = lappend(list1, lfirst(cell));
	}

	check_list_invariants(list1);
	return list1;
}

/*
 * This variant of list_concat_unique() operates upon lists of integers.
 */
List *
list_concat_unique_int(List *list1, const List *list2)
{
	ListCell   *cell;

	Assert(IsIntegerList(list1));
	Assert(IsIntegerList(list2));

	foreach(cell, list2)
	{
		if (!list_member_int(list1, lfirst_int(cell)))
			list1 = lappend_int(list1, lfirst_int(cell));
	}

	check_list_invariants(list1);
	return list1;
}

/*
 * This variant of list_concat_unique() operates upon lists of OIDs.
 */
List *
list_concat_unique_oid(List *list1, const List *list2)
{
	ListCell   *cell;

	Assert(IsOidList(list1));
	Assert(IsOidList(list2));

	foreach(cell, list2)
	{
		if (!list_member_oid(list1, lfirst_oid(cell)))
			list1 = lappend_oid(list1, lfirst_oid(cell));
	}

	check_list_invariants(list1);
	return list1;
}

/*
 * Remove adjacent duplicates in a list of OIDs.
 *
 * It is caller's responsibility to have sorted the list to bring duplicates
 * together, perhaps via list_sort(list, list_oid_cmp).
 */
void
list_deduplicate_oid(List *list)
{
	int			len;

	Assert(IsOidList(list));
	len = list_length(list);
	if (len > 1)
	{
		ListCell   *elements = list->elements;
		int			i = 0;

		for (int j = 1; j < len; j++)
		{
			if (elements[i].oid_value != elements[j].oid_value)
				elements[++i].oid_value = elements[j].oid_value;
		}
		list->length = i + 1;
	}
	check_list_invariants(list);
}

/*
 * Free all storage in a list, and optionally the pointed-to elements
 */
static void
list_free_private(List *list, bool deep)
{
	if (list == NIL)
		return;					/* nothing to do */

	check_list_invariants(list);

	if (deep)
	{
		for (int i = 0; i < list->length; i++)
			pfree(lfirst(&list->elements[i]));
	}
	if (list->elements != list->initial_elements)
		pfree(list->elements);
	pfree(list);
}
#endif

/*
 * Free all the cells of the list, as well as the list itself. Any
 * objects that are pointed-to by the cells of the list are NOT
 * free'd.
 *
 * On return, the argument to this function has been freed, so the
 * caller would be wise to set it to NIL for safety's sake.
 */
void
list_free(List *list)
{
#if PG_VERSION_NUM >= 130000	
	list_free_private(list, false);
#else
	list_destroy(list, NULL);
#endif		
}

/*
 * Free all the cells of the list, the list itself, and all the
 * objects pointed-to by the cells of the list (each element in the
 * list must contain a pointer to a palloc()'d region of memory!)
 *
 * On return, the argument to this function has been freed, so the
 * caller would be wise to set it to NIL for safety's sake.
 */
void
list_free_deep(List *list)
{
	/*
	 * A "deep" free operation only makes sense on a list of pointers.
	 */
	Assert(IsPointerList(list));
#if PG_VERSION_NUM >= 130000	
	list_free_private(list, true);
#else
	list_destroy(list, free);
#endif	
}

/*
 * Return a shallow copy of the specified list.
 */
#if PG_VERSION_NUM < 90200
List *
list_copy(List *oldlist)
{
	List	   *newlist;
	ListCell   *newlist_prev;
	ListCell   *oldlist_cur;

	if (oldlist == NIL)
		return NIL;

	newlist = new_list(oldlist->type);
	newlist->length = oldlist->length;

	/*
	 * Copy over the data in the first cell; new_list() has already allocated
	 * the head cell itself
	 */
	newlist->head->data = oldlist->head->data;

	newlist_prev = newlist->head;
	oldlist_cur = oldlist->head->next;
	while (oldlist_cur)
	{
		ListCell   *newlist_cur;

		newlist_cur = pgut_new(ListCell);
		newlist_cur->data = oldlist_cur->data;
		newlist_prev->next = newlist_cur;

		newlist_prev = newlist_cur;
		oldlist_cur = oldlist_cur->next;
	}

	newlist_prev->next = NULL;
	newlist->tail = newlist_prev;

	return newlist;
}
#elif PG_VERSION_NUM >= 130000
List *
list_copy(const List *oldlist)
{
	List	   *newlist;

	if (oldlist == NIL)
		return NIL;

	newlist = new_list(oldlist->type, oldlist->length);
	memcpy(newlist->elements, oldlist->elements,
		   newlist->length * sizeof(ListCell));

	check_list_invariants(newlist);
	return newlist;
}
#endif

/*
 * Return a shallow copy of the specified list, without the first N elements.
 */
#if PG_VERSION_NUM < 90200
List *
list_copy_tail(List *oldlist, int nskip)
{
	List	   *newlist;
	ListCell   *newlist_prev;
	ListCell   *oldlist_cur;

	if (nskip < 0)
		nskip = 0;				/* would it be better to elog? */

	if (oldlist == NIL || nskip >= oldlist->length)
		return NIL;

	newlist = new_list(oldlist->type);
	newlist->length = oldlist->length - nskip;

	/*
	 * Skip over the unwanted elements.
	 */
	oldlist_cur = oldlist->head;
	while (nskip-- > 0)
		oldlist_cur = oldlist_cur->next;

	/*
	 * Copy over the data in the first remaining cell; new_list() has already
	 * allocated the head cell itself
	 */
	newlist->head->data = oldlist_cur->data;

	newlist_prev = newlist->head;
	oldlist_cur = oldlist_cur->next;
	while (oldlist_cur)
	{
		ListCell   *newlist_cur;

		newlist_cur = pgut_new(ListCell);
		newlist_cur->data = oldlist_cur->data;
		newlist_prev->next = newlist_cur;

		newlist_prev = newlist_cur;
		oldlist_cur = oldlist_cur->next;
	}

	newlist_prev->next = NULL;
	newlist->tail = newlist_prev;

	return newlist;
}
#elif PG_VERSION_NUM >= 130000
List *
list_copy_tail(const List *oldlist, int nskip)
{
	List	   *newlist;

	if (nskip < 0)
		nskip = 0;				/* would it be better to elog? */

	if (oldlist == NIL || nskip >= oldlist->length)
		return NIL;

	newlist = new_list(oldlist->type, oldlist->length - nskip);
	memcpy(newlist->elements, &oldlist->elements[nskip],
		   newlist->length * sizeof(ListCell));

	check_list_invariants(newlist);
	return newlist;
}
#endif

#if PG_VERSION_NUM >= 130000
/*
 * Sort a list according to the specified comparator function.
 *
 * The list is sorted in-place.
 *
 * The comparator function is declared to receive arguments of type
 * const ListCell *; this allows it to use lfirst() and variants
 * without casting its arguments.  Otherwise it behaves the same as
 * the comparator function for standard qsort().
 *
 * Like qsort(), this provides no guarantees about sort stability
 * for equal keys.
 */
void
list_sort(List *list, list_sort_comparator cmp)
{
	typedef int (*qsort_comparator) (const void *a, const void *b);
	int			len;

	check_list_invariants(list);

	/* Nothing to do if there's less than two elements */
	len = list_length(list);
	if (len > 1)
		qsort(list->elements, len, sizeof(ListCell), (qsort_comparator) cmp);
}

/*
 * list_sort comparator for sorting a list into ascending OID order.
 */
int
list_oid_cmp(const ListCell *p1, const ListCell *p2)
{
	Oid			v1 = lfirst_oid(p1);
	Oid			v2 = lfirst_oid(p2);

	if (v1 < v2)
		return -1;
	if (v1 > v2)
		return 1;
	return 0;
}
#else /* PG_VERSION under 13 */
/* list_walk - apply walker for each item */
void
list_walk(List *list, void (*walker)())
{
	ListCell *cell;

	Assert(walker != NULL);

	foreach(cell, list)
		walker(lfirst(cell));
}

/*
 * Free all storage in a list, and optionally the pointed-to elements
 */
void
list_destroy(List *list, void (*walker)())
{
	ListCell   *cell;

	cell = list_head(list);
	while (cell != NULL)
	{
		ListCell   *tmp = cell;

		cell = lnext(cell);
		if (walker)
			walker(lfirst(tmp));
		free(tmp);
	}

	free(list);
}
#endif

#if !defined(USE_INLINE) && PG_VERSION_NUM < 90000 && !defined(__GNUC__)

ListCell *
list_head(List *l)
{
	return l ? l->head : NULL;
}

ListCell *
list_tail(List *l)
{
	return l ? l->tail : NULL;
}

int
list_length(List *l)
{
	return l ? l->length : 0;
}

#endif   /* ! USE_INLINE */


