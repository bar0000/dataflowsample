#pragma once

#include <process.h>
#include "Common.h"

namespace Common
{

ThreadBase::ThreadBase() :
	m_ThreadHandle(0)
{}
ThreadBase::~ThreadBase()
{
	DWORD exitcode = 0;

	if (m_ThreadHandle != 0) {
		while (::GetExitCodeThread(m_ThreadHandle, &exitcode) != 0)
		{
			if (exitcode != STILL_ACTIVE)
			{
				::CloseHandle(m_ThreadHandle);
				break;
			}
			::Sleep(0);
		}
	}
}

bool ThreadBase::Start()
{
	if (m_ThreadHandle == 0) {
		m_ThreadHandle = reinterpret_cast<HANDLE>(
			_beginthreadex(nullptr, 0, ThreadBase::callback, this, CREATE_SUSPENDED, nullptr)
			);
		if (m_ThreadHandle == 0)
		{
			return false;
		}

		::ResumeThread(m_ThreadHandle);
	}
	return true;
}



unsigned int __stdcall ThreadBase::callback(void* p)
{
	ThreadBase* base = static_cast<ThreadBase*>(p);

	base->Exec();
	base->Signal();

	return 0;
}

}