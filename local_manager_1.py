import json
import time
import logging
import tornado.httpclient
from tornado.web import Application, RequestHandler
from tornado.ioloop import IOLoop
from proton import Message
from threading import Thread
from proton.handlers import MessagingHandler
from proton.reactor import ApplicationEvent, Container, EventInjector, Selector

#############################################################################################
################################ Logging #################################
#############################################################################################
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def logger_setup(name, file_name, level=logging.DEBUG):
    """Setup different loggers here"""

    file_handler = logging.FileHandler(file_name)
    file_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(file_handler)
    

    return logger

general_log = logger_setup(' Local Manager 1 ','/logs/lm1.log')
time_log = logger_setup(' Timing Local Manager 1 ','/logs/lm1time.log')

#############################################################################################
################################ Logging #################################
#############################################################################################

class Subscriber(MessagingHandler):
    def __init__(self, server, receive_topic_names, config):
        super(Subscriber, self).__init__()
        self.server = server
        self.receive_topic_names = receive_topic_names
        self.receivers = dict()
        self.car_location = dict()
        self.lm_config = config
        self.filterstring = ""
        for qt in  self.lm_config["coverage_area"]:
            self.filterstring+="quadTree=\'"+qt+"\' OR "
        self.filterstring = self.filterstring[:-3]
        self.addr_change = ""
        self.need_update = 0
        self.car_ref_time = 0
        self.curr_location = ""
        self.end_zone = set()
        for ez in self.lm_config["transition_areas"]:
            self.end_zone.add(ez["to"])

    def on_start(self, event):
        conn = event.container.connect(self.server)
        for topic in self.receive_topic_names:
            self.receivers.update({topic:event.container.create_receiver(conn, 'topic://%s' % topic,\
                                   options=Selector(self.filterstring))})
        #"quadTree='A1' OR quadTree='A2' OR quadTree='A3' OR quadTree='B1' OR quadTree='B2' OR quadTree='B3'"

    def on_message(self, event):
        receive_msg = time.time()
        self.car_ref_time = float(event.message.properties["timestamp"])
        data = json.loads(event.message.body)
        car_id = data["Car_ID"]
        self.need_update = 0
        self.curr_location = event.message.properties["quadTree"]
        if car_id not in self.car_location:
            self.car_location.update({car_id:self.curr_location})
            #general_log.debug(self.car_location)
            general_log.info("Car "+str(car_id)+" is in "+self.curr_location)
        if self.curr_location in self.end_zone:
            #general_log.debug("Currently "+car_id+ " at "+ self.curr_location)

            for k in self.lm_config["transition_areas"]:
                self.addr_change = k["Address"]
                
                if k["from"] == self.car_location[car_id] and k["to"] == self.curr_location:
                    bjson = json.dumps({'Car_ID':car_id,\
                                     'EP':self.addr_change, 'timestamp_lm':time.time(), 'ref_timestamp_fc':self.car_ref_time})
                    change_ep_of_car(bjson)
                    send_change_ep_msg = time.time()
                    #time_log.info(str((send_change_ep_msg-self.car_ref_time)*1000)+" ms from msg sent from car to change of endpoint")
                    time_log.info(str((send_change_ep_msg-receive_msg)*1000)+" ms from reception of msg sent from car to change of endpoint")
                    #general_log.debug("CarID: "+car_id+ " Previous Position: "+self.car_location[car_id]+" Current position: "+ self.curr_location)
                    self.need_update = 1
                    #general_log.info("CarID: "+car_id+ " Previous Position: "+self.car_location[car_id]+" Current position: "+ self.curr_location+" Switching to:"+self.addr_change)
                    break

        if self.need_update == 0:
            self.car_location.update({car_id:self.curr_location})
            general_log.debug(self.car_location)
        else:
            del self.car_location[car_id]

AMQP_Addr=""

class MMtoLMConfig(RequestHandler):
    """ API SERVER for handling calls from the main manager regarding LM configuration"""
    
    def post(self, id):
        """Handles the behaviour of POST calls"""
        json_form = json.loads(self.request.body)
        self.write(json_form)
        AMQP_Addr = json_form["AMQP_Addr"]
        topics = ["FROM_CARS"]
        client_sub = Subscriber(AMQP_Addr, topics, json_form) 
        container = Container(client_sub)
        qpid_thread_sub = Thread(target=container.run)
        qpid_thread_sub.start()

    def put(self, id):
        """Handles the behaviour of PUT calls"""
        global items
        new_items = [item for item in items if item['id'] is not int(id)]
        items = new_items
        self.write({'message': 'Item with id %s was updated' % id})


    def delete(self, id):
        """Handles the behaviour of DELETE calls"""
        global items
        new_items = [item for item in items if item['id'] is not int(id)]
        items = new_items
        self.write({'message': 'Item with id %s was deleted' % id})
    


class MMtoLMCar(RequestHandler):
    """ API SERVER for handling calls from the main manager """

    def post(self, id):
        """Handles the behaviour of POST calls"""
        self.write(json.loads(self.request.body))
        #AMQP_Addr = json.loads(self.request.body)["local_manager_id"]
        json_form = json.loads(self.request.body)

    def put(self, id):
        """Handles the behaviour of PUT calls"""
        global items
        new_items = [item for item in items if item['id'] is not int(id)]
        items = new_items
        self.write({'message': 'Item with id %s was updated' % id})


    def delete(self, id):
        """Handles the behaviour of DELETE calls"""
        global items
        new_items = [item for item in items if item['id'] is not int(id)]
        items = new_items
        self.write({'message': 'Item with id %s was deleted' % id})



def change_ep_of_car(bjson):
    """ To change the endpoint of the car in case of chaging zones """
    #start_change_ep = time.time()
    try:
        response = http_client.fetch("http://response-router-1-service.clm-test.empower:3000/api/item/from_local_mgr_api/21"\
                                     ,method='POST',body=bjson)        
    except Exception as e:
        general_log.error("Error: %s" % e)
    else:
        general_log.debug(response.body)
        #general_log.debug(str((time.time()-start_change_ep)*1000)+" ms to POST")

def make_app():
  urls = [
    (r"/api/item/from_main_mgr_config_api/([^/]+)?", MMtoLMConfig),
    (r"/api/item/from_main_mgr_car_reg_api/([^/]+)?", MMtoLMCar)
  ]
  return Application(urls, debug=True)


  
if __name__ == '__main__':

  app = make_app()
  app.listen(3500)
  print("Started Local Manager 1 REST Server",flush=True)
  http_client = tornado.httpclient.HTTPClient()
  IOLoop.instance().start()
