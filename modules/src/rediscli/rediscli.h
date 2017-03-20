#pragma once

#include <string>
#include <vector>
#include <list>

#include "../Precompile.h"

namespace Modules {
namespace Redis {

class RedisClient;

class RedisCommandBase
{
public :
	RedisCommandBase(const std::string& command) ;
	virtual ~RedisCommandBase();

	bool Exec(redisContext* ctx);

	inline std::string& GetError() { return m_Error; }

protected :
	virtual bool parseReply() = 0;


protected:
	std::string	m_Command;
	redisReply*	m_Reply;
	std::string m_Error;
};

class RedisCommandKeys : public RedisCommandBase
{
public :
	RedisCommandKeys(const std::string& str) : RedisCommandBase( "keys *" )	{}
	std::vector<std::string> keys;
protected :
	bool parseReply();
};

class RedisCommandSet : public RedisCommandBase
{
public:
	RedisCommandSet(const std::string& keyvalue) :
		RedisCommandBase("set " + keyvalue),
		str()
	{}
	std::string str;
protected:
	inline bool parseReply() { str = m_Reply->str; return true; }
};


class RedisCommandGet : public RedisCommandBase
{
public:
	RedisCommandGet(const std::string& key) :
		RedisCommandBase("get " + key ),
		str(),
		i(0)
	{}
	std::string str;
	int64_t i;
protected:
	bool parseReply();
};


class RedisCommandJobInterface
{
public :
	virtual ~RedisCommandJobInterface() {}

	virtual void ExecCommand() = 0;
	virtual bool IsDone() = 0;

};
template<class T>
class RedisCommandJob : public RedisCommandJobInterface
{
public :
	RedisCommandJob(RedisClient& client, const std::string& cmdarg) :
		m_Client(client),
		m_Command(cmdarg),
		m_IsDone()
	{}

	void ExecCommand()
	{
		redisContext* ctx = m_Client.GetFreeContext();
		m_Command.Exec(ctx);
		m_Client.ReleaseContext(ctx);
		m_IsDone.Set(1);
	}

	bool IsDone() { return (m_IsDone.Get() == 1); }
	inline T& GetCommand() { return m_Command; }

private :
	RedisClient& m_Client;
	T m_Command;
	Common::Atomic m_IsDone;
};

class RedisCommandWorker : public Common::ThreadBase
{
public :
	RedisCommandWorker();
	virtual ~RedisCommandWorker();

	void Exec();

	template<class T>
	void AppendJob(T& job)
	{
		Common::MutexLocker lock(m_ListMutex);
		m_pAppendJobList->push_back(&job);
	}
private :
	Common::Mutex m_ListMutex;
	std::vector<RedisCommandJobInterface*> m_JobList[2];
	std::vector<RedisCommandJobInterface*>* m_pAppendJobList;
	std::vector<RedisCommandJobInterface*>* m_pCurrentJobList;
};

class RedisClient : public Common::ThreadBase
{
public:
	using KeysJob = RedisCommandJob<RedisCommandKeys>;
	using GetJob = RedisCommandJob<RedisCommandGet>;
	using SetJob = RedisCommandJob<RedisCommandSet>;


	RedisClient(const char* host, const int32_t port);
	virtual ~RedisClient();

	void Initialize();
	void Finalize();
	void Exec();

	redisContext* GetFreeContext();
	void ReleaseContext(redisContext* ctx);

	KeysJob& Keys();
	SetJob& Set(const char* key, const char* value);
	GetJob& Get(const char* key);

private :
	void createConnectionPool(const int32_t max);


private :
	std::string m_Host;
	int32_t m_Port;

	Common::Mutex m_ListMutex;
	std::vector<redisContext*> m_ConnectionPool;
	std::vector<redisContext*> m_FreeConnection;

	RedisCommandWorker m_WorkerThread[3];

	Common::Mutex m_JobMutex;
	std::vector<RedisCommandJobInterface*> m_Jobs[2];
	std::vector<RedisCommandJobInterface*>* m_AddJobs = nullptr;
	std::vector<RedisCommandJobInterface*>* m_CurrentJobs = nullptr;

};


}}