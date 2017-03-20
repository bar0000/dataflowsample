#include "rediscli.h"

namespace Modules {
namespace Redis {


RedisCommandBase::RedisCommandBase(const std::string& command) :
	m_Command(command),
	m_Reply(nullptr),
	m_Error()
{
}

RedisCommandBase::~RedisCommandBase()
{
}

bool RedisCommandBase::Exec(redisContext* ctx)
{
	m_Reply = reinterpret_cast<redisReply*>(redisCommand(ctx, m_Command.c_str()));
	bool r = false;
	if (m_Reply != nullptr) {
		if (m_Reply->type == REDIS_REPLY_ERROR) {
			m_Error = m_Reply->str;
		}
		else {
			r = parseReply();
			if (r == false)
				m_Error = "parseReply() Error";
		}
		freeReplyObject(m_Reply);
		m_Reply = nullptr;
	}
	return r;
}


bool RedisCommandKeys::parseReply()
{
	keys.clear();

	if (m_Reply->type != REDIS_REPLY_ARRAY)
		return false;

	for (size_t i = 0; i < m_Reply->elements; ++i)
	{
		keys.push_back(m_Reply->element[i]->str);
	}
	return true;
}

bool RedisCommandGet::parseReply()
{
	str.clear();
	i = 0;
	if (m_Reply->type == REDIS_REPLY_STRING)
		str = m_Reply->str;
	else if (m_Reply->type == REDIS_REPLY_INTEGER)
		i = m_Reply->integer;
	else
		return false;
	return true;
}

RedisCommandWorker::RedisCommandWorker()
{
	m_JobList[0].reserve(10);
	m_JobList[1].reserve(10);
	m_pAppendJobList = &m_JobList[0];
	m_pCurrentJobList = &m_JobList[1];
}
RedisCommandWorker::~RedisCommandWorker()
{}

void RedisCommandWorker::Exec()
{
	while (!IsStopped())
	{
		for (auto job : *m_pCurrentJobList)
		{
			job->ExecCommand();
		}
		m_pCurrentJobList->clear();
		{
			Common::MutexLocker lock(m_ListMutex);
			std::vector<RedisCommandJobInterface*>* tmp = m_pCurrentJobList;
			m_pCurrentJobList = m_pAppendJobList;
			m_pAppendJobList = tmp;
		}
		::Sleep(100);
	}
}

RedisClient::RedisClient(const char* host, const int port ) :
	m_Host(host),
	m_Port(port)
{
	m_AddJobs = &m_Jobs[0];
	m_CurrentJobs = &m_Jobs[1];
}

RedisClient::~RedisClient()
{
	Finalize();
}

void RedisClient::Initialize()
{
	createConnectionPool(10);

	m_Jobs[0].reserve(10);
	m_Jobs[1].reserve(10);

	for (int i = 0; i < 3; ++i) {
		m_WorkerThread[i].Start();
	}
}

void RedisClient::Finalize()
{
	for (int i = 0; i < 3; ++i) {
		m_WorkerThread[i].Stop();
		m_WorkerThread[i].Join();
	}

	for (auto jobs : m_Jobs) {
		for (auto j : jobs) {
			delete j;
		}
		jobs.clear();
	}
	for (auto c : m_ConnectionPool) {
		redisFree(c);
	}
	m_ConnectionPool.clear();

	Stop();
	Join();
}

void RedisClient::Exec()
{
	//Initialize();

	while (!IsStopped()) {
		for (auto j : *m_CurrentJobs) {
			j->ExecCommand();
		}
		m_CurrentJobs->clear();
		{
			Common::MutexLocker lock(m_JobMutex);
			std::vector<RedisCommandJobInterface*>* tmp = m_CurrentJobs;
			m_CurrentJobs = m_AddJobs;
			m_AddJobs = tmp;
		}
		::Sleep(100);
	}
}

redisContext* RedisClient::GetFreeContext()
{
	Common::MutexLocker lock(m_ListMutex);

	redisContext* r = nullptr;
	for (size_t i = 0, s = m_FreeConnection.size(); i < s; ++i) {
		r = m_FreeConnection[i];

		if (r != nullptr) {
			m_FreeConnection[i] = nullptr;
			break;
		}
	}
	return r;
}

void RedisClient::ReleaseContext(redisContext* ctx)
{
	Common::MutexLocker lock(m_ListMutex);

	redisContext* r = nullptr;
	for (size_t i = 0, s = m_FreeConnection.size(); i < s; ++i) {
		r = m_FreeConnection[i];

		if (r == nullptr) {
			m_FreeConnection[i] = ctx;
			break;
		}
	}
}

RedisClient::KeysJob& RedisClient::Keys()
{
	RedisClient::KeysJob* job = new RedisClient::KeysJob(*this, "");

	Common::MutexLocker lock(m_JobMutex);
	m_AddJobs->push_back(job);
	return *job;
}

RedisClient::SetJob& RedisClient::Set(const char* key, const char* value)
{
	std::string str = key;
	str += " ";
	str+= value;
	RedisClient::SetJob* job = new RedisClient::SetJob(*this, str );

	Common::MutexLocker lock(m_JobMutex);
	m_AddJobs->push_back(job);
	return *job;

}

RedisClient::GetJob& RedisClient::Get(const char* key)
{
	std::string str = key;
	RedisClient::GetJob* job = new RedisClient::GetJob(*this, str);

	Common::MutexLocker lock(m_JobMutex);
	m_AddJobs->push_back(job);
	return *job;
}

void RedisClient::createConnectionPool(const int max)
{
	Common::MutexLocker lock(m_ListMutex);

	m_ConnectionPool.reserve(max);
	m_FreeConnection.reserve(max);

	for (int i = 0; i < max; ++i) {
		redisContext* c = redisConnect(m_Host.c_str(), m_Port);
		if (c != nullptr)
			m_ConnectionPool.push_back(c);
	}
	for (auto i : m_ConnectionPool)
		m_FreeConnection.push_back(i);
}

}}