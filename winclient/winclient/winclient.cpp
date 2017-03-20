// winclient.cpp : コンソール アプリケーションのエントリ ポイントを定義します。
//

#include "stdafx.h"

#include <iostream>

int main(int argc, char *argv[])
{
	WSADATA wsaData;

	WSAStartup(MAKEWORD(1, 1), &wsaData);

	std::string host = "54.92.96.142";
	int32_t port = 6379;

	if (argc > 3) {
		host = argv[1];
		port = std::atoi(argv[2]);
	}
	else if (argc > 2) {
		host = argv[1];
	}
	Modules::Redis::RedisClient*  redisclient = new Modules::Redis::RedisClient(host.c_str(), port);

	redisclient->Initialize();
	redisclient->Start();

	std::string line;
	while (std::getline(std::cin, line)) {
		std::stringstream ss(line);
		std::string field;
		std::vector<std::string> split;
		while (std::getline(ss, field, ' '))
		{
			split.push_back(field);
		}

		if (split.empty())
			continue;

		std::transform(split[0].cbegin(), split[0].cend(), split[0].begin(), tolower);
		if (split[0] == "keys")
		{
			Modules::Redis::RedisClient::KeysJob& j = redisclient->Keys();
			while (!j.IsDone()) {
				::Sleep(100);
			}
			if (j.GetCommand().GetError().empty())
			{
				for (auto k : j.GetCommand().keys)
				{
					std::cout << "\"" + k + "\"" << std::endl;
				}
			}
			else
				std::cout << "error: " + j.GetCommand().GetError() << std::endl;
		}
		else if (split[0] == "get")
		{
			if (split.size() == 2) {
				Modules::Redis::RedisClient::GetJob& j = redisclient->Get(split[1].c_str());
				while (!j.IsDone()) {
					::Sleep(100);
				}
				if (j.GetCommand().GetError().empty())
					std::cout << "\"" + split[1] + "\" = \"" + j.GetCommand().str << +"\"" << std::endl;
				else
					std::cout << "error: " + j.GetCommand().GetError() << std::endl;
			}
		}
		else if (split[0] == "set")
		{
			if (split.size() == 3) {
				Modules::Redis::RedisClient::SetJob& j = redisclient->Set(split[1].c_str(), split[2].c_str());
				while (!j.IsDone()) {
					::Sleep(100);
				}
				if (j.GetCommand().GetError().empty())
					std::cout << "set \"" + split[1] + "\" = \"" + split[2] << +"\"" << std::endl;
				else
					std::cout << "error: " + j.GetCommand().GetError() << std::endl;
			}
		}
		else if (split[0] == "exit" || split[0] == "quit")
			break;
	}

	delete redisclient;
	
    return 0;
}

