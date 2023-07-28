import time
from spade.agent import Agent
from spade.behaviour import OneShotBehaviour
from spade.message import Message


class SenderAgent(Agent):
    class InformBehavA(OneShotBehaviour):
        async def run(self):
            msg = Message(to="erdmbyrktr@anonym.im")
            msg.set_metadata("performative", "inform")
            msg.set_metadata("ontology", "myOntology")
            msg.set_metadata("language", "OWL-S")
            msg.body = " ".join([word for i, word in enumerate(self.words) if i % 2 == 0])
            await self.send(msg)
            print("Subscriber1:\n" + "\n".join([word for i, word in enumerate(self.words) if i % 2 == 0]))
            self.exit_code = "Job Finished!"
            await self.agent.stop()

        def __init__(self, words):
            super().__init__()
            self.words = words

    class InformBehavB(OneShotBehaviour):
        async def run(self):
            msg = Message(to="erdmbyrktr1@anonym.im")
            msg.set_metadata("performative", "inform")
            msg.set_metadata("ontology", "myOntology")
            msg.set_metadata("language", "OWL-S")
            msg.body = " ".join([word for i, word in enumerate(self.words) if i % 2 == 1])
            await self.send(msg)
            print("\nSubscriber2:\n" + "\n".join([word for i, word in enumerate(self.words) if i % 2 == 1]))
            self.exit_code = "Job Finished!"
            await self.agent.stop()

        def __init__(self, words):
            super().__init__()
            self.words = words

    async def setup(self):
        print("SenderAgent started \n")
        words = "Hello Agent Oriented Programming".split()
        self.b_a = self.InformBehavA(words)
        self.b_b = self.InformBehavB(words)
        self.add_behaviour(self.b_a)
        self.add_behaviour(self.b_b)


if __name__ == "__main__":
    agent = SenderAgent("intruder@anonym.im", "password")
    future = agent.start()
    future.result()

    while agent.is_alive():
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            agent.stop()
            break
    print("\nAgent finished with exit code: {}".format(agent.b_a.exit_code))

    print()