class ScheduleUtil(object):
    @staticmethod
    def get_cost_func(to_schedule, phy_node):
        """

        :param to_schedule:
        :param phy_node:
        :return:
        """
        if to_schedule == "none":
            return None
        elif to_schedule == "cpu":
            return lambda req: req.get_res_consumption(phy_node.cpu_res)
        elif to_schedule == "io":
            return lambda req: req.get_res_consumption(phy_node.io_res)
        elif to_schedule == "net":
            return lambda req: req.get_res_consumption(phy_node.net_res)
        elif to_schedule == "cpu_net":
            return lambda req: [
                req.get_res_consumption(phy_node.cpu_res) / (phy_node.cpu_res.rate * len(phy_node.cpu_res.workers)),
                req.get_res_consumption(phy_node.net_res) / (phy_node.net_res.rate * len(phy_node.net_res.workers))]
        elif to_schedule == "all":
            return lambda req: [
                req.get_res_consumption(phy_node.cpu_res) / (phy_node.cpu_res.rate * len(phy_node.cpu_res.workers)),
                req.get_res_consumption(phy_node.io_res) / (phy_node.io_res.rate * len(phy_node.io_res.workers)),
                req.get_res_consumption(phy_node.net_res) / (phy_node.net_res.rate * len(phy_node.net_res.workers))]
        elif to_schedule == "all_4":
            return lambda req: [
                req.get_res_consumption(phy_node.cpu_res) / (phy_node.cpu_res.rate * len(phy_node.cpu_res.workers)),
                req.get_res_consumption(phy_node.io_res) / (phy_node.io_res.rate * len(phy_node.io_res.workers)),
                req.get_res_consumption(phy_node.net_res) / (phy_node.net_res.rate * len(phy_node.net_res.workers)),
                0]
        else:
            raise Exception("Unknown to_schedule cost type: " + str(to_schedule))
