#pragma once

namespace Common
{

class Mutex
{
public :

	Mutex()
	{
		::InitializeCriticalSection(&m_CS);
	}

	virtual ~Mutex()
	{
		::DeleteCriticalSection(&m_CS);
	}

	inline void Lock()
	{
		::EnterCriticalSection(&m_CS);
	}
	
	inline bool TryLock()
	{
		return (::TryEnterCriticalSection(&m_CS) != 0);
	}

	inline void Unlock()
	{
		::LeaveCriticalSection(&m_CS);
	}

private:
	CRITICAL_SECTION m_CS;
};

class MutexLocker
{
public:
	MutexLocker( Mutex& m ) : m_Mutex(m) { m_Mutex.Lock(); }
	~MutexLocker() { m_Mutex.Unlock(); }
private:
	Mutex& m_Mutex;
};


class Atomic
{
public :
	Atomic() : m_value(0) {}
	virtual ~Atomic() {}

	inline void Atomic::Set(uint32_t value)
	{
		_InterlockedExchange(&m_value, static_cast<LONG>(value));
	}

	inline uint32_t Atomic::Get() const
	{
		return static_cast<uint32_t>(_InterlockedExchangeAdd(const_cast<volatile LONG*>(&m_value), 0));
	}

	inline uint32_t CompareExchange(uint32_t value, uint32_t comperand)
	{
		return static_cast<uint32_t>(_InterlockedCompareExchange(&m_value, static_cast<LONG>(value), static_cast<LONG>(comperand)));
	}

private:
	volatile LONG m_value;
};

class AtomicLocker
{
public :
	AtomicLocker(Atomic& a) : m_Atomic(a) { m_Atomic.CompareExchange(1, 0); }
	~AtomicLocker() { m_Atomic.CompareExchange(0, 1); }
private :
	Atomic& m_Atomic;
};

class Event
{
public :
	Event()
	{
		m_EventHandle = ::CreateEvent(nullptr, TRUE, FALSE, nullptr);
	}
	virtual ~Event()
	{
		::CloseHandle(m_EventHandle);
	}

	inline void Wait()
	{
		DWORD result = ::WaitForSingleObject(m_EventHandle, INFINITE);
	}
	inline void Signal()
	{
		::SetEvent(m_EventHandle);
	}
	inline void Reset()
	{
		::ResetEvent(m_EventHandle);
	}

private:
	HANDLE m_EventHandle;
};

class ThreadBase
{
public:
	ThreadBase();
	virtual ~ThreadBase();
	
	virtual void Exec() = 0;

	bool Start();

	inline void Join()
	{
		m_Event.Wait();
	}

	inline void Signal()
	{
		m_Event.Signal();
	}

	inline void Stop()
	{
		m_Stopped.Set(1);
	}

	inline bool IsStopped()
	{
		return (m_Stopped.Get() != 0);
	}

private :
	static unsigned int __stdcall callback(void* argument);

	HANDLE m_ThreadHandle;
	Atomic m_Stopped;
	Event m_Event;
};


}
