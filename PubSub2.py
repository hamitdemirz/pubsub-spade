from spade.agent import Agent
from spade.behaviour import PeriodicBehaviour, OneShotBehaviour
from spade.message import Message
import pandas as pd
import time
from spade.template import Template


class PowerPublisher(Agent):
    class PowerBehaviour(PeriodicBehaviour):
        counter = 0

        def __init__(self, period):
            super().__init__(period)
            self.df = pd.read_csv("C:\\Users\\erdmb\\Downloads\\archive\\T1.csv")
            self.df = self.df[["Date/Time", "LV ActivePower (kW)"]]

        async def run(self):
            msg = Message(to="subscribererd@anonym.im")
            row = self.df.iloc[self.counter]
            msg.body = f"{row['Date/Time']},{row['LV ActivePower (kW)']}"
            await self.send(msg)
            self.counter += 1
            if self.counter == len(self.df):
                self.counter = 0

    async def setup(self):
        b = self.PowerBehaviour(period=0.01)
        self.add_behaviour(b)
        self.counter = 0
        print("Power publisher started...")


class WindSpeedPublisher(Agent):
    class WindSpeedBehaviour(PeriodicBehaviour):
        counter = 0

        def __init__(self, period):
            super().__init__(period)
            self.df = pd.read_csv("C:\\Users\\erdmb\\Downloads\\archive\\T1.csv")
            self.df = self.df[["Date/Time", "Wind Speed (m/s)"]]

        async def run(self):
            msg = Message(to="subscribererd@anonym.im")
            row = self.df.iloc[self.counter]
            msg.body = f"{row['Date/Time']},{row['Wind Speed (m/s)']}"
            await self.send(msg)
            self.counter += 1
            if self.counter == len(self.df):
                self.counter = 0

    async def setup(self):
        b = self.WindSpeedBehaviour(period=0.01)
        self.add_behaviour(b)


class ActivePowerPublisher(Agent):
    class ActivePowerBehaviour(PeriodicBehaviour):
        counter = 0

        def __init__(self, period):
            super().__init__(period)
            self.df = pd.read_csv("C:\\Users\\erdmb\\Downloads\\archive\\T1.csv")
            self.df = self.df[["Date/Time", "LV ActivePower (kW)"]]

        async def run(self):
            msg = Message(to="subscribererd@anonym.im")
            row = self.df.iloc[self.counter]
            msg.body = f"{row['Date/Time']},{row['LV ActivePower (kW)']}"
            await self.send(msg)
            self.counter += 1
            if self.counter == len(self.df):
                self.counter = 0

    async def setup(self):
        b = self.ActivePowerBehaviour(period=0.01)
        self.add_behaviour(b)


class WindDirectionPublisher(Agent):
    class WindDirectionBehaviour(PeriodicBehaviour):
        counter = 0

        def __init__(self, period):
            super().__init__(period)
            self.df = pd.read_csv("C:\\Users\\erdmb\\Downloads\\archive\\T1.csv")
            self.df = self.df[["Date/Time", "Wind Direction (°)"]]

        async def run(self):
            msg = Message(to="subscribererd@anonym.im")
            row = self.df.iloc[self.counter]
            msg.body = f"{row['Date/Time']},{row['Wind Direction (°)']}"
            await self.send(msg)
            self.counter += 1
            if self.counter == len(self.df):
                self.counter = 0

        async def setup(self):
            b = self.ActivePowerBehaviour(period=0.01)
            self.add_behaviour(b)


class Subscriber(Agent):
    class SubscriberBehaviour(OneShotBehaviour):
        def __init__(self):
            super().__init__()
            self.df = pd.read_csv("C:\\Users\\erdmb\\Downloads\\archive\\T1.csv")
            self.df = self.df[["Date/Time", "Theoretical_Power_Curve (KWh)"]]
            self.received_messages = []

        async def setup(self):
            template = Template()
            template.set_metadata("performative", "inform")
            template.set_metadata("ontology", "scada-data")

            self.add_template(template)

        async def run(self):
            # Wait for messages
            msg = await self.receive(timeout=0.01)
            if msg:
                self.received_messages.append(msg.body)

            # Check if all data has been received
            if len(self.received_messages) == len(self.df):
                # Match received data with theoretical data
                data = []
                for i, message in enumerate(self.received_messages):
                    time, value = message.split(",")
                    row = self.df.iloc[i]
                    data.append([time, value, row["Theoretical_Power_Curve (KWh)"]])

                # Save data to file
                output_df = pd.DataFrame(data,
                                         columns=["Date/Time", "LV ActivePower (kW)", "Theoretical_Power_Curve (KWh)"])
                output_df.to_csv("output.csv", index=False)
                print("Output file saved successfully.")

    async def setup(self):
        b = self.SubscriberBehaviour()
        self.add_behaviour(b)


if __name__ == "__main__":
    agent = PowerPublisher("publisher1erd@anonym.im", "password")
    future = agent.start()
    future.result()

    agent = WindSpeedPublisher("publisher2erd@anonym.im", "password")
    future = agent.start()
    future.result()

    agent = ActivePowerPublisher("publisher3erd@anonym.im", "password")
    future = agent.start()
    future.result()


    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            PowerPublisher.stop()
            WindSpeedPublisher.stop()
            ActivePowerPublisher.stop()
            WindDirectionPublisher.stop()
        break
    print("\nAgent finished with exit code: {}".format(agent.exit_code))
