int pgwin32_select(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timeval * timeout);

static char *
GetSharedMemName(void)
{
	char	   *retptr;
	DWORD		bufsize;
	DWORD		r;
	char	   *cp;

	bufsize = GetFullPathName(DataDir, 0, NULL, NULL);
	if (bufsize == 0)
		return NULL;

	retptr = malloc(bufsize + 18);		/* 18 for Global\PostgreSQL: */
	if (retptr == NULL)
		return NULL;

	strcpy(retptr, "Global\\PostgreSQL:");
	r = GetFullPathName(DataDir, bufsize, retptr + 18, NULL);
	if (r == 0 || r > bufsize)
		return NULL;

	for (cp = retptr; *cp; cp++)
		if (*cp == '\\')
			*cp = '/';

	return retptr;
}

bool
PGSharedMemoryIsInUse(unsigned long id1, unsigned long id2)
{
	char	   *szShareMem;
	HANDLE		hmap;

	szShareMem = GetSharedMemName();
	if (szShareMem == NULL)
	{
		free(szShareMem);
		return true;	/* if can't stat, be conservative */
	}

	hmap = OpenFileMapping(FILE_MAP_READ, FALSE, szShareMem);

	free(szShareMem);

	if (hmap == NULL)
		return false;

	CloseHandle(hmap);
	return true;
}

static void
TranslateSocketError(void)
{
	switch (WSAGetLastError())
	{
		case WSANOTINITIALISED:
		case WSAENETDOWN:
		case WSAEINPROGRESS:
		case WSAEINVAL:
		case WSAESOCKTNOSUPPORT:
		case WSAEFAULT:
		case WSAEINVALIDPROVIDER:
		case WSAEINVALIDPROCTABLE:
		case WSAEMSGSIZE:
			errno = EINVAL;
			break;
		case WSAEAFNOSUPPORT:
			errno = EAFNOSUPPORT;
			break;
		case WSAEMFILE:
			errno = EMFILE;
			break;
		case WSAENOBUFS:
			errno = ENOBUFS;
			break;
		case WSAEPROTONOSUPPORT:
		case WSAEPROTOTYPE:
			errno = EPROTONOSUPPORT;
			break;
		case WSAECONNREFUSED:
			errno = ECONNREFUSED;
			break;
		case WSAEINTR:
			errno = EINTR;
			break;
		case WSAENOTSOCK:
			errno = EBADFD;
			break;
		case WSAEOPNOTSUPP:
			errno = EOPNOTSUPP;
			break;
		case WSAEWOULDBLOCK:
			errno = EWOULDBLOCK;
			break;
		case WSAEACCES:
			errno = EACCES;
			break;
		case WSAENOTCONN:
		case WSAENETRESET:
		case WSAECONNRESET:
		case WSAESHUTDOWN:
		case WSAECONNABORTED:
		case WSAEDISCON:
			errno = ECONNREFUSED;		/* ENOTCONN? */
			break;
		default:
			errno = EINVAL;
	}
}

int
pgwin32_select(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timeval * timeout)
{
	WSAEVENT	events[FD_SETSIZE * 2]; /* worst case is readfds totally
										 * different from writefds, so
										 * 2*FD_SETSIZE sockets */
	SOCKET		sockets[FD_SETSIZE * 2];
	int			numevents = 0;
	int			i;
	int			r;
	FD_SET		outreadfds;
	FD_SET		outwritefds;
	int			nummatches = 0;

	Assert(writefds == NULL);
	Assert(exceptfds == NULL);
	Assert(timeout == NULL);

	FD_ZERO(&outreadfds);
	FD_ZERO(&outwritefds);

	/* Now set up for an actual select */

	if (readfds != NULL)
	{
		for (i = 0; i < readfds->fd_count; i++)
		{
			events[numevents] = WSACreateEvent();
			sockets[numevents] = readfds->fd_array[i];
			numevents++;
		}
	}
	for (i = 0; i < numevents; i++)
	{
		int			flags = 0;

		if (readfds && FD_ISSET(sockets[i], readfds))
			flags |= FD_READ | FD_ACCEPT | FD_CLOSE;

		if (WSAEventSelect(sockets[i], events[i], flags) == SOCKET_ERROR)
		{
			TranslateSocketError();
			for (i = 0; i < numevents; i++)
				WSACloseEvent(events[i]);
			return -1;
		}
	}

	r = WaitForMultipleObjectsEx(numevents, events, FALSE, WSA_INFINITE, TRUE);
	if (r != WAIT_TIMEOUT && r != WAIT_IO_COMPLETION && r != (WAIT_OBJECT_0 + numevents))
	{
		/*
		 * We scan all events, even those not signalled, in case more than one
		 * event has been tagged but Wait.. can only return one.
		 */
		WSANETWORKEVENTS resEvents;

		for (i = 0; i < numevents; i++)
		{
			ZeroMemory(&resEvents, sizeof(resEvents));
			if (WSAEnumNetworkEvents(sockets[i], events[i], &resEvents) == SOCKET_ERROR)
			{
				TranslateSocketError();
				return -1;
			}
			/* Read activity? */
			if (readfds && FD_ISSET(sockets[i], readfds))
			{
				if ((resEvents.lNetworkEvents & FD_READ) ||
					(resEvents.lNetworkEvents & FD_ACCEPT) ||
					(resEvents.lNetworkEvents & FD_CLOSE))
				{
					FD_SET(sockets[i], &outreadfds);
					nummatches++;
				}
			}
		}
	}

	/* Clean up all handles */
	for (i = 0; i < numevents; i++)
	{
		WSAEventSelect(sockets[i], events[i], 0);
		WSACloseEvent(events[i]);
	}

	if (r == WSA_WAIT_TIMEOUT)
	{
		if (readfds)
			FD_ZERO(readfds);
		return 0;
	}

	if (r == WAIT_OBJECT_0 + numevents)
	{
		errno = EINTR;
		if (readfds)
			FD_ZERO(readfds);
		return -1;
	}

	/* Overwrite socket sets with our resulting values */
	if (readfds)
		memcpy(readfds, &outreadfds, sizeof(fd_set));
	return nummatches;
}
