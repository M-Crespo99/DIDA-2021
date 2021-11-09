using System;
namespace PCS
{

    public static class Scheduler
    {
        public static void IntervalInSeconds(double interval, Action task)
        {
            interval = interval/3600;
            SchedulerService.Instance.ScheduleTask(interval, task);
        }
    }
}