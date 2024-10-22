


//
// Target
//

pub trait Target {
    fn capture_wireframes(&self);

    fn capture_screenshot(&self);
}


//
// Reporter
//

pub struct Reporter {
    target: Box<dyn Target + Send + Sync>,

    is_enabled: bool,
    reporting_interval_rate: Duration,
    reporting_interval: Option<Interval>,

    is_enabled_flag: BoolWatch<ResourceUtilizationEnabledFlag>,
    reporting_interval_flag: DurationWatch<ResourceUtilizationReportingIntervalFlag>,
}



